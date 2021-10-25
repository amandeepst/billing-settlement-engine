package com.worldpay.pms.bse.engine.transformations;

import static com.google.common.collect.Iterators.transform;

import com.worldpay.pms.bse.domain.common.ErrorCatalog;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.InputCorrectionEncodings;
import com.worldpay.pms.bse.engine.domain.billcorrection.BillCorrectionService;
import com.worldpay.pms.bse.engine.transformations.model.IdGenerator;
import com.worldpay.pms.bse.engine.transformations.model.billerror.BillError;
import com.worldpay.pms.bse.engine.transformations.model.billerror.BillErrorDetail;
import com.worldpay.pms.bse.engine.transformations.model.billrelationship.BillRelationshipRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillCorrectionRow;
import com.worldpay.pms.spark.core.factory.Factory;
import java.util.Iterator;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import scala.Tuple2;
import scala.Tuple3;

@UtilityClass
@Slf4j
public class BillCorrecting {

  public static Dataset<CompleteBillResult> computeBillCorrection(Dataset<Tuple2<InputBillCorrectionRow, BillLineRow[]>> bills,
      Dataset<BillTax> billTaxes, Broadcast<Factory<BillCorrectionService>> billCorrectionServiceFactoryBroadcast,
      Broadcast<Factory<IdGenerator>> idGeneratorFactory) {

    return bills
        .joinWith(billTaxes, bills.col("_1.billId").equalTo(billTaxes.col("billId")), "left")
        .map(tuple -> new Tuple3<>(tuple._1()._1, tuple._1._2(), tuple._2), InputCorrectionEncodings.BILL_CORRECTION_TUPLE_ENCODER)
        .mapPartitions(
            p -> computeBillCorrection(p, billCorrectionServiceFactoryBroadcast.getValue().build().get(),
                idGeneratorFactory.getValue().build().get()),
            Encodings.COMPLETE_BILL_RESULT_ENCODER);
  }

  public static Dataset<BillRelationshipRow> computeBillRelationship(Dataset<CompleteBillResult> bills,
      Broadcast<Factory<BillCorrectionService>> billCorrectionServiceFactoryBroadcast) {
    return bills
        .filter(CompleteBillResult::isComplete)
        .mapPartitions(
            billResult -> computeBillRelationship(billResult, billCorrectionServiceFactoryBroadcast.getValue().build().get()),
            Encodings.BILL_RELATIONSHIP_ENCODER)
        .alias("Corrected Bill Relationship");
  }

  private static Iterator<BillRelationshipRow> computeBillRelationship(Iterator<CompleteBillResult> billRowIterator,
      BillCorrectionService billCorrectionService) {
    return transform(billRowIterator, billRow -> billCorrectionService.createBillRelationship(billRow.getCompleteBill()));
  }

  private static Iterator<CompleteBillResult> computeBillCorrection(Iterator<Tuple3<InputBillCorrectionRow, BillLineRow[], BillTax>> p,
      BillCorrectionService billCorrectionService, IdGenerator idGenerator) {
    return transform(p, t -> {
      if (t._1().getPartyId() == null) {
        return CompleteBillResult.error(generateBillError(t._1().getBillId(), t._1().getCorrectionEventId()), null);
      }
      try {
        val correctedBill = billCorrectionService.correctBill(BillRow.from(t._1(), t._2()), idGenerator.generateId());
        val correctedBillTax = (t._3() == null) ? null : billCorrectionService.correctBillTax(correctedBill.getBillId(), t._3());

        return CompleteBillResult.complete(correctedBill.withBillTax(correctedBillTax), null);
      }catch (Exception ex) {
        log.error("Unhandled exception when processing correction  `{}`", t._1(), ex);
        return CompleteBillResult.error(
            generateInvalidCorrectionError(t._1().getBillId(), t._1().getCorrectionEventId(), ExceptionUtils.getStackTrace(ex)),
            null);
      }
    });
  }

  private static BillError generateBillError(String billId, String correctionEventId) {
    val billErrorDetails = new BillErrorDetail[]{BillErrorDetail.of(ErrorCatalog.noBillFoundForCorrection(billId, correctionEventId))};
    return BillError.error(billId, TransformationUtils.getCurrentDate(), TransformationUtils.getCurrentDate(), 1, billErrorDetails);
  }

  private static BillError generateInvalidCorrectionError(String billId, String correctionEventId, String errorMessage) {
    val billErrorDetails = new BillErrorDetail[]{BillErrorDetail.of(
        ErrorCatalog.invalidCorrection(billId, correctionEventId, errorMessage))};
    return BillError.error(billId, TransformationUtils.getCurrentDate(), TransformationUtils.getCurrentDate(), 1, billErrorDetails);
  }

}
