package com.worldpay.pms.bse.engine.domain.billcorrection;

import com.worldpay.pms.bse.domain.common.Utils;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTaxDetail;
import com.worldpay.pms.bse.engine.transformations.model.billrelationship.BillRelationshipRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import java.util.Arrays;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class BillCorrectionService {

  public BillRow correctBill(BillRow bill, String billId) {
    return bill.toBuilder()
        .billId(billId)
        .previousBillId(bill.getBillId())
        .billAmount(bill.getBillAmount().negate())
        .billLines(correctBillLinesAmounts(bill.getBillLines()))
        .build();
  }

  public BillRelationshipRow createBillRelationship(BillRow bill) {
    return BillRelationshipRow.builder()
        .parentBillId(bill.getPreviousBillId())
        .childBillId(bill.getBillId())
        .paidInvoice(bill.getCorrectionPaidInvoice())
        .eventId(bill.getCorrectionEventId())
        .relationshipTypeId("51")
        .reuseDueDate("N")
        .type(bill.getCorrectionType())
        .reasonCd(bill.getCorrectionReasonCd())
        .build();
  }

  public BillTax correctBillTax(String correctedBillId, BillTax billTax) {
    return billTax.toBuilder()
        .billTaxId(Utils.generateId())
        .billId(correctedBillId)
        .billTaxDetails(correctBillTaxDetails(billTax.getBillTaxDetails()))
        .build();
  }

  private BillLineRow[] correctBillLinesAmounts(BillLineRow[] billLines) {
    return Arrays.stream(billLines)
        .map(billLine -> billLine.toBuilder()
            .billLineId(Utils.generateId())
            .previousBillLine(billLine.getBillLineId())
            .totalAmount(billLine.getTotalAmount().negate())
            .billLineCalculations(correctBillLineCalculations(billLine.getBillLineCalculations()))
            .billLineServiceQuantities(correctBillLineServiceQuantities(billLine.getBillLineServiceQuantities()))
            .build())
        .toArray(BillLineRow[]::new);
  }

  private BillLineCalculationRow[] correctBillLineCalculations(BillLineCalculationRow[] billLineCalculations) {
    return Arrays.stream(billLineCalculations)
        .map(billLineCalculation -> billLineCalculation.toBuilder()
            .billLineCalcId(Utils.generateId())
            .amount(billLineCalculation.getAmount().negate())
            .build())
        .toArray(BillLineCalculationRow[]::new);
  }

  private BillLineServiceQuantityRow[] correctBillLineServiceQuantities(BillLineServiceQuantityRow[] billLineServiceQuantities) {
    return Arrays.stream(billLineServiceQuantities)
        .map(billLineServiceQuantity ->
            "F_M_AMT".equals(billLineServiceQuantity.getServiceQuantityCode()) ?
                billLineServiceQuantity.toBuilder().serviceQuantity(billLineServiceQuantity.getServiceQuantity().negate()).build() :
                billLineServiceQuantity)
        .toArray(BillLineServiceQuantityRow[]::new);
  }

  private BillTaxDetail[] correctBillTaxDetails(BillTaxDetail[] billTaxDetails) {
    return Arrays.stream(billTaxDetails)
        .map(billTaxDetail -> billTaxDetail.toBuilder()
            .netAmount(billTaxDetail.getNetAmount().negate())
            .taxAmount(billTaxDetail.getTaxAmount().negate())
            .build())
        .toArray(BillTaxDetail[]::new);
  }

}
