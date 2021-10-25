package com.worldpay.pms.bse.engine.transformations;

import static com.google.common.collect.Iterators.forArray;
import static com.google.common.collect.Iterators.transform;
import static com.worldpay.pms.bse.engine.transformations.model.billerror.BillErrorDetail.UNHANDLED_EXCEPTION_CODE;
import static com.worldpay.pms.spark.core.TransformationsUtils.instrumentMapPartitions;

import com.worldpay.pms.bse.domain.BillProcessingService;
import com.worldpay.pms.bse.domain.common.BillLineDomainError;
import com.worldpay.pms.bse.domain.common.Utils;
import com.worldpay.pms.bse.domain.model.billtax.BillLineTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBill;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillDecision;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLine;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLineCalculation;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLineServiceQuantity;
import com.worldpay.pms.bse.domain.model.completebill.UpdateBillDecision;
import com.worldpay.pms.bse.domain.model.mincharge.PendingMinimumCharge;
import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.data.BillingRepository;
import com.worldpay.pms.bse.engine.transformations.model.IdGenerator;
import com.worldpay.pms.bse.engine.transformations.model.billerror.BillError;
import com.worldpay.pms.bse.engine.transformations.model.billerror.BillErrorDetail;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetailRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.PendingMinChargeRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import com.worldpay.pms.pce.common.DomainError;
import com.worldpay.pms.spark.core.factory.Factory;
import io.vavr.collection.Array;
import io.vavr.collection.Seq;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.Iterator;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import scala.Tuple2;

@UtilityClass
@Slf4j
public class BillProcessing {

  private static final int MINUS_ONE = -1;
  private static final String EMPTY = "";

  /**
   * Attempt to complete aggregated pending bills. Possible outcomes, for each row, are:
   * <ul>
   *  <li>complete bill     - BillRow and BillLineDetailRow[] are on CompleteBillResult
   *  <li>not complete bill - PendingBillRow and BilLineDetailRow[] is on CompleteBillResult
   *  <li>error bill        - BillError, PendingBillRow and BilLineDetailRow[] is on CompleteBillResult
   * </ul>
   *
   * @param pendingBillDataset       the dataset of pending bills
   * @param billProcessingService    the complete bill service factory
   * @param billingRepositoryFactory static data repo factory
   * @return dataset of results containing data needed to persist for each outcome
   */
  public static Dataset<CompleteBillResult> completePendingBills(Dataset<Tuple2<PendingBillRow, PendingMinChargeRow[]>> pendingBillDataset,
      Broadcast<Factory<BillProcessingService>> billProcessingService,
      Broadcast<Factory<BillingRepository>> billingRepositoryFactory,
      Broadcast<Factory<IdGenerator>> idGeneratorFactory) {

    return pendingBillDataset
        .mapPartitions(instrumentMapPartitions(rows -> toCompleteBillResult(rows, billProcessingService.getValue().build().get(),
            billingRepositoryFactory.getValue().build().get(), idGeneratorFactory.getValue().build().get())),
            Encodings.COMPLETE_BILL_RESULT_ENCODER);
  }

  /**
   * Update bill cycle id and end date on the pending bills read from the database. The end date on the pending bill will then be used as
   * part of the aggregation key with the new billable items and also for deciding whether to complete the bill or not.
   *
   * @param pendingBillDataset    the dataset of pending bills
   * @param billProcessingService the complete bill service factory
   * @return dataset of results containing data needed in further processing for each outcome
   */
  public static Dataset<UpdateBillResult> updatePendingBills(Dataset<PendingBillRow> pendingBillDataset,
      Broadcast<Factory<BillProcessingService>> billProcessingService) {
    return pendingBillDataset
        .mapPartitions(instrumentMapPartitions(rows -> updatePendingBills(rows, billProcessingService.getValue().build().get())),
            Encodings.UPDATE_BILL_RESULT_ENCODER);
  }

  public static Dataset<BillLineDetailRow> getBillLineDetails(Dataset<CompleteBillResult> billResults) {
    return
        billResults
            .filter(CompleteBillResult::isPending)
            .flatMap(billResult -> forArray(billResult.getPendingBill().billLineDetails()), Encodings.BILL_LINE_DETAIL_ROW_ENCODER)
            .union(billResults
                .filter(CompleteBillResult::isComplete)
                .flatMap(billResult -> forArray(billResult.getCompleteBill().getBillLineDetails()), Encodings.BILL_LINE_DETAIL_ROW_ENCODER)
            );
  }

  private static Iterator<CompleteBillResult> toCompleteBillResult(Iterator<Tuple2<PendingBillRow, PendingMinChargeRow[]>> rows,
      BillProcessingService billProcessingService, BillingRepository billingRepository, IdGenerator idGenerator) {
    return transform(rows,
        tuple2 -> toCompleteBillResultWithLogging(tuple2._1() == null ? Option.none() : Option.of(tuple2._1()),
            tuple2._2() == null ? Stream.empty() : toMinChargeSeq(tuple2._2()),
            billProcessingService, billingRepository, idGenerator));
  }

  private static Iterator<UpdateBillResult> updatePendingBills(Iterator<PendingBillRow> rows,
      BillProcessingService billProcessingService) {
    return transform(rows, pendingBill -> updatePendingBill(pendingBill, billProcessingService));
  }

  private static CompleteBillResult toCompleteBillResultWithLogging(Option<PendingBillRow> pendingBill,
      Seq<PendingMinimumCharge> pendingMinimumCharges,
      BillProcessingService billProcessingService, BillingRepository billingRepository, IdGenerator idGenerator) {

    CompleteBillResult result = toCompleteBillResult(pendingBill, pendingMinimumCharges, billProcessingService, billingRepository,
        idGenerator);


    if (result.isError()) {
      BillError billError = result.getBillError();
      for (BillErrorDetail billErrorDetail : billError.getBillErrorDetails()) {
        log.warn("Bill line with billLineId={} billId={} accountId={} subAccountId={} isFirstFailure={} firstFailure={} attempts={} "
                + "failed with code={} reason={}",
            billErrorDetail.getBillLineId(),
            billError.getBillId(),
            getErrorAccountId(pendingBill),
            getErrorSubAccountId(pendingBill),
            billError.isFirstFailure(),
            billError.getFirstFailureOn(),
            billError.getRetryCount(),
            billErrorDetail.getCode(),
            billErrorDetail.getMessage()
        );
      }
    } else {
      Option<BillRow> completeBill = Option.of(result.getCompleteBill());
      log.info("Complete bill result: billId={} accountId={} subAccountId={} isComplete={} isPending={} linesNo={} lineDetailsNo={}",
          getResultBillId(pendingBill, completeBill, pendingMinimumCharges),
          getResultAccountId(pendingBill, completeBill),
          getResultSubAccountId(pendingBill, completeBill),
          result.isComplete(),
          result.isPending(),
          getResultLinesNo(pendingBill, completeBill),
          getResultLineDetailsNo(pendingBill)
      );
    }
    return result;
  }

  private static CompleteBillResult toCompleteBillResult(Option<PendingBillRow> pendingBill,
      Seq<PendingMinimumCharge> pendingMinimumCharges,
      BillProcessingService billProcessingService, BillingRepository billingRepository, IdGenerator idGenerator) {
    CompleteBillResult result;
    try {
      result =  billProcessingService.completeBill(pendingBill.getOrElse((PendingBillRow) null), pendingMinimumCharges)
          .fold(
              errors -> getErrorCompleteBillResult(pendingBill, pendingMinimumCharges, errors),
              completeBillDecision -> getSuccessCompleteBillResult(pendingBill, completeBillDecision, billingRepository, idGenerator)
          );
    } catch (Exception ex) {
      log.error("Unhandled exception when completing pending bill `{}`", pendingBill, ex);
      if (pendingBill.isDefined()) {
        result = CompleteBillResult.error(
            getBillError(
                getPendingBillWithErrorDetails(pendingBill.get()),
                BillLineDomainError.from(DomainError.of(UNHANDLED_EXCEPTION_CODE, ex.getMessage(), pendingBill)).toSeq(),
                ex),
            pendingBill.get()
        );
      } else {
        result = CompleteBillResult.error(
            getBillError(
                pendingMinimumCharges,
                BillLineDomainError.from(DomainError.of(UNHANDLED_EXCEPTION_CODE, ex.getMessage(), pendingMinimumCharges.get(0))).toSeq(),
                ex),
            null
        );
      }
    }

    // Make sure we carry over the pending minimum charges for the case when we don't complete the daily bill
    // (for example, when running with different processing group)
    if (result.getPendingMinimumCharges() == null && result.isPending() && !result.isError()) {
      result = result.withPendingMinimumCharges(pendingMinimumCharges
          .filter(pendingMinCharge -> pendingMinCharge.getApplicableCharges().compareTo(BigDecimal.ZERO) != 0)
          .toJavaArray(PendingMinimumCharge[]::new));
    }
    return result;
  }

  private static UpdateBillResult updatePendingBill(PendingBillRow pendingBill, BillProcessingService billProcessingService) {

    try {
      return billProcessingService.updateBill(pendingBill)
          .fold(
              errors -> getErrorUpdateBillResult(pendingBill, errors),
              updateBillDecision -> getSuccessUpdateBillResult(pendingBill, updateBillDecision)
          );
    } catch (Exception ex) {
      log.error("Unhandled exception when updating pending bill `{}`", pendingBill, ex);
      return UpdateBillResult.error(pendingBill, getBillError(
          getPendingBillWithErrorDetails(pendingBill),
          BillLineDomainError.from(DomainError.of(UNHANDLED_EXCEPTION_CODE, ex.getMessage(), pendingBill)).toSeq(),
          ex)
      );
    }
  }

  private static Seq<PendingMinimumCharge> toMinChargeSeq(PendingMinChargeRow[] pendingMinCharges) {
    return Stream.of(pendingMinCharges)
        .map(BillProcessing::toPendingMinChargeFromRow);
  }

  private static PendingMinimumCharge toPendingMinChargeFromRow(PendingMinChargeRow pendingMinChargeRow) {
    return new PendingMinimumCharge(
        pendingMinChargeRow.getBillPartyId(),
        pendingMinChargeRow.getLegalCounterparty(),
        pendingMinChargeRow.getTxnPartyId(),
        pendingMinChargeRow.getMinChargeStartDate() == null ? null : pendingMinChargeRow.getMinChargeStartDate().toLocalDate(),
        pendingMinChargeRow.getMinChargeEndDate() == null ? null : pendingMinChargeRow.getMinChargeEndDate().toLocalDate(),
        pendingMinChargeRow.getMinChargeType(),
        pendingMinChargeRow.getApplicableCharges(),
        pendingMinChargeRow.getBillDate() == null ? null : pendingMinChargeRow.getBillDate().toLocalDate(),
        pendingMinChargeRow.getCurrency());
  }

  private static CompleteBillResult getErrorCompleteBillResult(Option<PendingBillRow> pendingBill,
      Seq<PendingMinimumCharge> pendingMinimumCharges, Seq<BillLineDomainError> errors) {

    return pendingBill.fold(
        () -> CompleteBillResult.error(getBillError(pendingMinimumCharges, errors), null),
        pendingBillRow -> {
          PendingBillRow updatedPendingBill = getPendingBillWithErrorDetails(pendingBillRow);
          return CompleteBillResult.error(getBillError(updatedPendingBill, errors), updatedPendingBill);
        });
  }

  private static UpdateBillResult getErrorUpdateBillResult(PendingBillRow pendingBill, Seq<BillLineDomainError> errors) {
    PendingBillRow updatedPendingBill = getPendingBillWithErrorDetails(pendingBill);
    return UpdateBillResult.error(updatedPendingBill, getBillError(updatedPendingBill, errors));
  }

  private static PendingBillRow getPendingBillWithErrorDetails(PendingBillRow pendingBill) {
    if (pendingBill.getFirstFailureOn() == null) {
      pendingBill.setFirstFailureOn(TransformationUtils.getCurrentDate());
    }
    pendingBill.setRetryCount((short) (pendingBill.getRetryCount() + 1));
    return pendingBill;
  }

  private static BillError getBillError(Seq<PendingMinimumCharge> pendingMinimumCharges, Seq<BillLineDomainError> errors) {
    BillErrorDetail[] billErrorDetails = getBillErrorDetails(errors);
    return BillError.error(pendingMinimumCharges.get(0).getTxnPartyId(), TransformationUtils.getCurrentDate(),
        TransformationUtils.getCurrentDate(), 999, billErrorDetails);
  }

  private static BillError getBillError(Seq<PendingMinimumCharge> pendingMinimumCharges, Seq<BillLineDomainError> errors,
      Throwable throwable) {
    BillErrorDetail[] billErrorDetails = getBillErrorDetails(errors, throwable);
    return BillError.error(pendingMinimumCharges.get(0).getTxnPartyId(), TransformationUtils.getCurrentDate(),
        TransformationUtils.getCurrentDate(), 999, billErrorDetails);
  }

  private static BillErrorDetail[] getBillErrorDetails(Seq<BillLineDomainError> errors) {
    return errors
        .map(err -> BillErrorDetail.of(err.getBillLineId(), err.getDomainError()))
        .toJavaArray(BillErrorDetail[]::new);
  }

  private static CompleteBillResult getSuccessCompleteBillResult(Option<PendingBillRow> pendingBill, CompleteBillDecision completeBillDecision,
      BillingRepository billingRepository, IdGenerator idGenerator) {

    if (completeBillDecision.isShouldComplete()) {
      CompleteBill completeBill = completeBillDecision.getCompleteBill();
      return CompleteBillResult.complete(getBill(pendingBill, completeBill, billingRepository, idGenerator), completeBillDecision.getPendingMinimumCharges());
    }

    return pendingBill.fold(
        () -> CompleteBillResult.pendingMinimumCharges(completeBillDecision.getPendingMinimumCharges()),
        pendingBillRow -> CompleteBillResult.pending(pendingBillRow, completeBillDecision.getPendingMinimumCharges())
    );
  }

  private static UpdateBillResult getSuccessUpdateBillResult(PendingBillRow pendingBill, UpdateBillDecision updateBillDecision) {
    PendingBillRow updatedPendingBill = pendingBill;
    if (updateBillDecision.isShouldUpdate()) {
      updatedPendingBill = pendingBill.toBuilder()
          .billCycleId(updateBillDecision.getBillCycleId())
          .scheduleEnd(Date.valueOf(updateBillDecision.getScheduleEnd()))
          .build();
    }
    return UpdateBillResult.success(updatedPendingBill);
  }

  private static BillError getBillError(PendingBillRow pendingBill, Seq<BillLineDomainError> errors) {
    BillErrorDetail[] billErrorDetails = getBillErrorDetails(errors);
    return BillError.error(pendingBill, billErrorDetails);
  }

  private static BillError getBillError(PendingBillRow pendingBill, Seq<BillLineDomainError> errors, Throwable throwable) {
    BillErrorDetail[] billErrorDetails = getBillErrorDetails(errors, throwable);

    return BillError.error(pendingBill, billErrorDetails);
  }

  private static BillErrorDetail[] getBillErrorDetails(Seq<BillLineDomainError> errors, Throwable throwable) {
    return errors
        .map(err -> BillErrorDetail.of(err.getBillLineId(), throwable))
        .toJavaArray(BillErrorDetail[]::new);
  }

  private static BillRow getBill(Option<PendingBillRow> pendingBill, CompleteBill completeBill, BillingRepository billingRepository,
      IdGenerator idGenerator) {
    return BillRow.from(
        Utils.getOrDefault(completeBill.getBillId(), idGenerator::generateId),
        completeBill,
        getBillLines(completeBill.getBillLines(), billingRepository, completeBill.getBillTax()),
        pendingBill.getOrElse(() -> null)
    );
  }

  private static BillLineRow[] getBillLines(CompleteBillLine[] completeBillLines, BillingRepository billingRepository, BillTax billTax) {
    return Array.of(completeBillLines)
        .map(completeBillLine -> {
          String billLineTaxStatus = (billTax == null) ? null : billTax.getBillLineTaxStatus(completeBillLine.getBillLineId());

          return BillLineRow
              .from(completeBillLine,
                  getBillLineCalculations(completeBillLine.getBillLineId(), completeBillLine.getBillLineCalculations(), billingRepository,
                      billTax),
                  getBillLineServiceQuantities(completeBillLine.getBillLineServiceQuantities(), billingRepository),
                  billingRepository.getProductDescription(completeBillLine.getProductIdentifier()),
                  billLineTaxStatus);
        })
        .toJavaArray(BillLineRow[]::new);
  }

  private static BillLineCalculationRow[] getBillLineCalculations(String billLineId, CompleteBillLineCalculation[] billLineCalculations,
      BillingRepository billingRepository, BillTax billTax) {
    return Array.of(billLineCalculations)
        .map(billLineCalculation -> {
              BillLineTax billLineTax = (billTax == null) ? null : billTax.getBillLineTax(billLineId);
              return BillLineCalculationRow.from(
                  billLineCalculation,
                  billingRepository.getCalcLineTypeDescription(billLineCalculation.getCalculationLineType()),
                  billLineTax);
            }
        )
        .toJavaArray(BillLineCalculationRow[]::new);
  }

  private static BillLineServiceQuantityRow[] getBillLineServiceQuantities(
      CompleteBillLineServiceQuantity[] completeBillLineServiceQuantities, BillingRepository billingRepository) {
    return Array.of(completeBillLineServiceQuantities)
        .map(svcQty -> BillLineServiceQuantityRow.from(svcQty, billingRepository.getSvcQtyDescription(svcQty.getServiceQuantityTypeCode())))
        .toJavaArray(BillLineServiceQuantityRow[]::new);
  }

  private static String getResultBillId(Option<PendingBillRow> pendingBill, Option<BillRow> completeBill,
      Seq<PendingMinimumCharge> pendingMinCharges) {
    return completeBill.fold(
        () -> pendingBill.fold(
            () -> pendingMinCharges == null || pendingMinCharges.length() == 0 ? EMPTY : pendingMinCharges.get(0).getBillPartyId(),
            PendingBillRow::getBillId),
        BillRow::getBillId
    );
  }

  private static String getResultAccountId(Option<PendingBillRow> pendingBill, Option<BillRow> completeBill) {
    return completeBill.fold(
        () -> pendingBill.fold(
            () -> EMPTY,
            PendingBillRow::getAccountId),
        BillRow::getAccountId
    );
  }

  private static String getResultSubAccountId(Option<PendingBillRow> pendingBill, Option<BillRow> completeBill) {
    return completeBill.fold(
        () -> pendingBill.fold(
            () -> EMPTY,
            PendingBillRow::getBillSubAccountId),
        BillRow::getBillSubAccountId
    );
  }

  private static Integer getResultLinesNo(Option<PendingBillRow> pendingBill, Option<BillRow> completeBill) {
    return completeBill.fold(
        () -> pendingBill.fold(
            () -> MINUS_ONE,
            p -> p.getPendingBillLines().length),
        b -> b.getBillLines().length
    );
  }

  private static Integer getResultLineDetailsNo(Option<PendingBillRow> pendingBill) {
    return pendingBill.fold(
        () -> MINUS_ONE,
        p -> Stream.of(p.getPendingBillLines())
            .map(l -> l.getBillLineDetails() == null ? MINUS_ONE : l.getBillLineDetails().length)
            .sum()
            .intValue()
    );
  }

  private static String getErrorAccountId(Option<PendingBillRow> pendingBill) {
    return pendingBill.fold(() -> EMPTY, PendingBillRow::getAccountId);
  }

  private static String getErrorSubAccountId(Option<PendingBillRow> pendingBill) {
    return pendingBill.fold(() -> EMPTY, PendingBillRow::getBillSubAccountId);
  }
}