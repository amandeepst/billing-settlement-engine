package com.worldpay.pms.bse.engine.transformations;

import static com.google.common.collect.Iterators.transform;
import static com.worldpay.pms.bse.engine.transformations.aggregation.PendingBillAggregationUtils.aggregateByKey;

import com.worldpay.pms.bse.domain.BillableItemProcessingService;
import com.worldpay.pms.bse.domain.account.BillingAccount;
import com.worldpay.pms.bse.domain.common.Utils;
import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.data.BillingRepository;
import com.worldpay.pms.bse.engine.transformations.aggregation.PendingBillAggregation;
import com.worldpay.pms.bse.engine.transformations.model.IdGenerator;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemRow;
import com.worldpay.pms.bse.engine.transformations.model.billprice.BillPriceRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetail;
import com.worldpay.pms.bse.engine.transformations.model.failedbillableitem.FailedBillableItem;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import com.worldpay.pms.spark.core.factory.Factory;
import io.vavr.collection.Array;
import io.vavr.control.Either;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;

@UtilityClass
@Slf4j
public class PendingBillProcessing {

  private static final String F_M_AMT = "F_M_AMT";
  private static final int SETTLEMENT_SUB_LVL_KEY_VALUE_SIZE = 2;
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyMMdd");

  public static Dataset<PendingBillResult> toPendingBillResult(Dataset<BillableItemRow> billableItemDataset,
      Broadcast<Factory<BillableItemProcessingService>> billableItemProcessingServiceFactory,
      Broadcast<Factory<BillingRepository>> repositoryFactoryBroadcast) {
    return billableItemDataset.mapPartitions(
        p -> toPendingBillResult(p, billableItemProcessingServiceFactory.value().build().get(),
            repositoryFactoryBroadcast.value().build().get()), Encodings.PENDING_BILL_RESULT_ENCODER);
  }

  public static Dataset<PendingBillRow> generateIds(Dataset<PendingBillRow> pendingBillDataset,
      Broadcast<Factory<IdGenerator>> idGeneratorFactory) {

    return pendingBillDataset.mapPartitions(p -> generateIds(p,idGeneratorFactory.getValue().build().get()), Encodings.PENDING_BILL_ENCODER);
  }

  private static Iterator<PendingBillResult> toPendingBillResult(Iterator<BillableItemRow> rows,
      BillableItemProcessingService billableItemProcessingService, BillingRepository billingRepository) {
    return transform(rows, row -> toPendingBillResult(row, billableItemProcessingService, billingRepository));
  }

  private static Iterator<PendingBillRow> generateIds(Iterator<PendingBillRow> rows, IdGenerator idGenerator) {
    return transform(rows, row -> generateIds(row, idGenerator));
  }

  private static PendingBillResult toPendingBillResult(BillableItemRow billableItem,
      BillableItemProcessingService billableItemProcessingService, BillingRepository billingRepository) {
    PendingBillResult result = new PendingBillResult(toPendingBill(billableItem, billableItemProcessingService, billingRepository));

    if (result.isFailure()) {
      FailedBillableItem failedBillableItem = result.getFailure();
      log.warn("Billable item with billableItemId={} subAccountId={} isFirstFailure={} firstFailure={} attempts={} "
              + "failed with code={} reason={}",
          billableItem.getBillableItemId(),
          billableItem.getSubAccountId(),
          failedBillableItem.isFirstFailure(),
          failedBillableItem.getFirstFailureOn(),
          failedBillableItem.getRetryCount(),
          failedBillableItem.getCode(),
          failedBillableItem.getReason()
      );
    } else if (result.isPreviouslyFailedAndFixed()) {
      log.info("Billable item with billableItemId={} fixed after attempts={}",
          billableItem.getBillableItemId(),
          billableItem.getRetryCount() + 1
      );
    }
    return result;
  }

  private static PendingBillRow generateIds(PendingBillRow pendingBill, IdGenerator idGenerator) {
    String billId = Utils.getOrDefault(pendingBill.getBillId(), idGenerator::generateId);
    //<ProductId, BillLineId>
    Map<String, String> billLineIds = new HashMap<>();
    return pendingBill.toBuilder()
        .billId(billId)
        .pendingBillLines(Array.of(pendingBill.getPendingBillLines())
            .map(pendingBillLine -> generateIdForBillLines(pendingBillLine, billLineIds))
            .toJavaArray(PendingBillLineRow[]::new))
        .billPrices(Array.of(pendingBill.getBillPrices())
            .map(billPrice -> billPrice.toBuilder()
                .billId(billId)
                .billLineDetailId(billLineIds.get(billPrice.getProductId()))
                .build())
            .toJavaArray(BillPriceRow[]::new))
        .build();
  }

  private static PendingBillLineRow generateIdForBillLines(PendingBillLineRow pendingBillLine, Map<String, String> billLineIds) {
    String billLineId = Utils.getOrDefault(pendingBillLine.getBillLineId(), Utils::generateId);
    //keep the bill line id in a map, to stamp it on Bill Price too
    billLineIds.put(pendingBillLine.getProductIdentifier(), billLineId);
    return pendingBillLine.toBuilder()
        .billLineId(billLineId)
        .pendingBillLineCalculations(Array.of(pendingBillLine.getPendingBillLineCalculations())
            .map(pendingBillLineCalculation -> pendingBillLineCalculation.toBuilder()
                .billLineCalcId(Utils.getOrDefault(pendingBillLineCalculation.getBillLineCalcId(), Utils::generateId))
                .build()).toJavaArray(PendingBillLineCalculationRow[]::new))
        .build();
  }

  private static Either<FailedBillableItem, PendingBillRow> toPendingBill(BillableItemRow billableItemRow,
      BillableItemProcessingService billableItemProcessingService, BillingRepository billingRepository) {

    try {
      return billableItemProcessingService.process(billableItemRow)
          .map(result -> toPendingBill(billableItemRow, result.getBillingAccount(), result.isCalculationCorrect(), billingRepository))
          .mapError(acc -> FailedBillableItem.of(billableItemRow, acc))
          .toEither();
    } catch (Exception ex) {
      log.error("Unhandled exception when creating pending bill from billableItemId={}", billableItemRow.getBillableItemId(), ex);
      return Either.left(FailedBillableItem.of(billableItemRow, ex));
    }
  }

  private static PendingBillRow toPendingBill(BillableItemRow billableItem, BillingAccount billingAccount, Boolean isLineCalcCorrect,
      BillingRepository billingRepository) {

    String billReference = getBillReference(billableItem, billingAccount);
    String billGranularity = getBillGranularity(billableItem, billingAccount);

    val pendingBillLineCalculations = aggregateByKey(
        getPendingBillLineCalculations(billableItem, billingRepository), PendingBillLineCalculationRow.class,
        PendingBillLineCalculationRow[]::new, PendingBillLineCalculationRow::aggregationKey, PendingBillAggregation::reduce);
    val billableItemServiceQuantities = billableItem.getBillableItemServiceQuantity().getServiceQuantities();
    val pendingBillLineServiceQuantities = aggregateByKey(
        getPendingBillLineServiceQuantities(billableItemServiceQuantities, billableItem.getMerchantAmountSignage()),
        PendingBillLineServiceQuantityRow.class,
        PendingBillLineServiceQuantityRow[]::new, PendingBillLineServiceQuantityRow::aggregationKey, PendingBillAggregation::reduce);
    val billPrices = getBillPrice(billableItem, billingAccount, billReference, billGranularity);
    val pendingBillLineRow = getPendingBillLineRow(billableItem, billingAccount, pendingBillLineCalculations, billableItemServiceQuantities,
        pendingBillLineServiceQuantities);

    return getPendingBillRow(billableItem, billingAccount, pendingBillLineRow, billPrices, billReference, billGranularity,
        isLineCalcCorrect);
  }

  private static BillPriceRow[] getBillPrice(BillableItemRow billableItem, BillingAccount billingAccount,
      String billReference, String billGranularity) {

    if (billableItem.isMiscBillableItem()) {
      return Arrays.stream(billableItem.getBillableItemLines())
          .map(billableItemLine -> BillPriceRow.from(billableItem, billableItemLine, billingAccount, billReference, billGranularity))
          .toArray(BillPriceRow[]::new);
    }
    //return empty array to avoid null checks on transformations
    return new BillPriceRow[]{};
  }

  private static PendingBillLineServiceQuantityRow[] getPendingBillLineServiceQuantities(
      Map<String, BigDecimal> billableItemServiceQuantities, Integer merchantAmountSignage) {
    // negate funding amount if signage is negative
    if (billableItemServiceQuantities.get(F_M_AMT) != null && merchantAmountSignage < 0) {
      Map<String, BigDecimal> newMap = new HashMap<>(billableItemServiceQuantities);
      newMap.put(F_M_AMT, billableItemServiceQuantities.get(F_M_AMT).negate());

      return newMap.entrySet()
          .stream().map(entry -> new PendingBillLineServiceQuantityRow(entry.getKey(), entry.getValue(), -1L))
          .toArray(PendingBillLineServiceQuantityRow[]::new);
    }

    return billableItemServiceQuantities.entrySet()
        .stream().map(entry -> new PendingBillLineServiceQuantityRow(entry.getKey(), entry.getValue(), -1L))
        .toArray(PendingBillLineServiceQuantityRow[]::new);
  }

  private static PendingBillLineCalculationRow[] getPendingBillLineCalculations(BillableItemRow billableItem,
      BillingRepository billingRepository) {
    return Arrays.stream(billableItem.getBillableItemLines())
        .map(billableItemLineRow -> new PendingBillLineCalculationRow(
            null,
            billableItemLineRow.getCalculationLineClassification(),
            billableItemLineRow.getCalculationLineType(),
            billableItemLineRow.getAmount(),
            billingRepository.getCalculationTypeBillDisplay(billableItemLineRow.getCalculationLineType()),
            billableItemLineRow.getRateType(),
            billableItemLineRow.getRateValue(),
            billableItemLineRow.getPartitionId()
        )).toArray(PendingBillLineCalculationRow[]::new);
  }

  private static PendingBillLineRow getPendingBillLineRow(BillableItemRow billableItem, BillingAccount billingAccount,
      PendingBillLineCalculationRow[] pendingBillLineCalculations, Map<String, BigDecimal> billableItemServiceQuantities,
      PendingBillLineServiceQuantityRow[] pendingBillLineServiceQuantities) {

    BigDecimal fundingAmount = billableItemServiceQuantities.get(F_M_AMT);

    return new PendingBillLineRow(
        null,
        billingAccount.getChildPartyId(),
        billableItem.getProductClass(),
        billableItem.getProductId(),
        billableItem.getPriceCurrency(),
        billableItem.getFundingCurrency(),
        billableItem.getMerchantAmountSignage() < 0 && fundingAmount != null ? fundingAmount.negate() : fundingAmount,
        billableItem.getTransactionCurrency(),
        billableItemServiceQuantities.get("TXN_AMT"),
        billableItemServiceQuantities.get("TXN_VOL"),
        billableItem.getPriceLineId(),
        billableItem.getMerchantCode(),
        billableItem.getPartitionId(),
        BillLineDetail.array(billableItem.getBillableItemId(), billableItem.getAggregationHash()),
        pendingBillLineCalculations,
        pendingBillLineServiceQuantities
    );
  }

  private static PendingBillRow getPendingBillRow(BillableItemRow billableItem, BillingAccount billingAccount,
      PendingBillLineRow pendingBillLineRow, BillPriceRow[] billPrices, String billReference, String billGranularity,
      Boolean isLineCalcCorrect) {
    String settlementSubLevelType = null;
    String settlementSubLevelValue = null;
    if (billableItem.getSettlementLevelType() != null) {
      String[] settlementLevel = billableItem.getSettlementLevelType().split("\\|");

      if (settlementLevel.length == SETTLEMENT_SUB_LVL_KEY_VALUE_SIZE) {
        settlementSubLevelType = settlementLevel[0].trim();
        settlementSubLevelValue = settlementLevel[1].trim();
      }
    }

    return new PendingBillRow(
        null,
        billingAccount.getPartyId(),
        billingAccount.getLegalCounterparty(),
        billingAccount.getAccountId(),
        billingAccount.getSubAccountId(),
        billingAccount.getAccountType(),
        billingAccount.getBusinessUnit(),
        billingAccount.getBillingCycle().getCode(),
        Date.valueOf(billingAccount.getBillingCycle().getStartDate()),
        Date.valueOf(billingAccount.getBillingCycle().getEndDate()),
        billingAccount.getCurrency(),
        billReference,
        billableItem.getAdhocBillFlag(),
        settlementSubLevelType,
        settlementSubLevelValue,
        billGranularity,
        billableItem.getGranularityKeyValue(),
        billableItem.getDebtDate(),
        billableItem.getDebtMigrationType(),
        billableItem.getOverpaymentIndicator(),
        billableItem.getReleaseWAFIndicator(),
        billableItem.getReleaseReserveIndicator(),
        billableItem.getFastestSettlementIndicator(),
        billableItem.getCaseIdentifier(),
        billableItem.getIndividualPaymentIndicator(),
        billableItem.getPaymentNarrative(),
        isLineCalcCorrect ? "N" : "Y",
        null,
        (short) 0,
        billableItem.getPartitionId(),
        new PendingBillLineRow[]{pendingBillLineRow},
        billPrices,
        billableItem.getBillableItemId(),
        billableItem.getIlmDate(),
        billableItem.getFirstFailureOn(),
        billableItem.getRetryCount()
    );
  }

  private static String getBillReference(BillableItemRow billableItem, BillingAccount billingAccount) {
    String granularityKeyVal = "Y".equalsIgnoreCase(billableItem.getIndividualPaymentIndicator())
        ? billableItem.getBillableItemId() : billableItem.getGranularityKeyValue();
    return billingAccount.getAccountId() +
        Objects.toString(billingAccount.getBillingCycle().getStartDate(), "") +
        Objects.toString(granularityKeyVal, "") +
        billableItem.getAdhocBillFlag() +
        billableItem.getReleaseReserveIndicator() +
        billableItem.getReleaseWAFIndicator() +
        billableItem.getFastestSettlementIndicator();
  }

  private static String getBillGranularity(BillableItemRow billableItem, BillingAccount billingAccount) {
    if (billableItem.isMiscBillableItem()) {
      return DATE_FORMATTER.format(billingAccount.getBillingCycle().getEndDate()) + billingAccount.getAccountId() +
          billableItem.getGranularity();
    }
    return billableItem.getGranularity();
  }
}