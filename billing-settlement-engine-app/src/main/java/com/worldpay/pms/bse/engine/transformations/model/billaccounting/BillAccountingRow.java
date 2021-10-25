package com.worldpay.pms.bse.engine.transformations.model.billaccounting;

import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTaxDetail;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import io.vavr.collection.Seq;
import io.vavr.collection.Stream;
import java.math.BigDecimal;
import java.sql.Date;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BillAccountingRow {

  @NonNull
  private String partyId;
  @NonNull
  private String accountId;
  @NonNull
  private String accountType;
  @NonNull
  private String billSubAccountId;
  @NonNull
  private String subAccountType;
  @NonNull
  private String currency;
  @NonNull
  private BigDecimal billAmount;
  @NonNull
  private String billId;
  @NonNull
  private String billLineId;
  @NonNull
  private BigDecimal totalLineAmount;
  @NonNull
  private String legalCounterparty;
  @NonNull
  private String businessUnit;
  @NonNull
  private String billClass;
  @NonNull
  private String calculationLineId;
  @NonNull
  private String type;
  private String calculationLineType;
  @NonNull
  private BigDecimal calculationLineAmount;
  @NonNull
  private Date billDate;

  public static BillAccountingRow[] from(BillRow bill) {
    return fromBill(bill).toJavaArray(BillAccountingRow[]::new);
  }

  public static BillAccountingRow[] from(BillRow bill, BillTax billTax) {
    return fromBillAndBillTax(bill, billTax).toJavaArray(BillAccountingRow[]::new);
  }

  private static Seq<BillAccountingRow> fromBill(BillRow bill) {
    return Stream.of(bill.getBillLines())
        .flatMap(billLine -> BillAccountingRow.fromBillLine(billLine, bill));
  }

  private static Seq<BillAccountingRow> fromBillLine(BillLineRow billLine, BillRow bill) {
    return Stream.of(billLine.getBillLineCalculations())
        .map(billLineCalc -> BillAccountingRow.fromBillLineCalc(billLineCalc, billLine, bill));
  }

  private static Seq<BillAccountingRow> fromBillAndBillTax(BillRow bill, BillTax billTax) {
    return Stream.of(billTax.getBillTaxDetails())
        .filter(billTaxDetail -> billTaxDetail.getTaxRate().compareTo(BigDecimal.ZERO) != 0)
        .map(billTaxDetail -> BillAccountingRow.fromBillTaxDetail(bill, billTax, billTaxDetail));
  }

  private static BillAccountingRow fromBillLineCalc(BillLineCalculationRow billLineCalc, BillLineRow billLine, BillRow bill) {
    return new BillAccountingRow(
        bill.getPartyId(),
        bill.getAccountId(),
        bill.getAccountType(),
        bill.getBillSubAccountId(),
        bill.getAccountType(),
        bill.getCurrencyCode(),
        bill.getBillAmount(),
        bill.getBillId(),
        billLine.getBillLineId(),
        billLine.getTotalAmount(),
        bill.getLegalCounterparty(),
        bill.getBusinessUnit(),
        "BILLING",
        billLineCalc.getBillLineCalcId(),
        billLine.getProductId(),
        billLineCalc.getCalculationLineClassification(),
        billLineCalc.getAmount(),
        bill.getBillDate()
    );
  }

  private static BillAccountingRow fromBillTaxDetail(BillRow bill, BillTax billTax, BillTaxDetail billTaxDetail) {
    return new BillAccountingRow(
        bill.getPartyId(),
        bill.getAccountId(),
        bill.getAccountType(),
        bill.getBillSubAccountId(),
        bill.getAccountType(),
        bill.getCurrencyCode(),
        bill.getBillAmount(),
        bill.getBillId(),
        billTax.getBillTaxId(),
        billTaxDetail.getTaxAmount(),
        bill.getLegalCounterparty(),
        bill.getBusinessUnit(),
        "TAX",
        billTax.getBillTaxId(),
        billTax.getTaxAuthority() + "-" + billTax.getTaxType(),
        "TAX",
        billTaxDetail.getTaxAmount(),
        bill.getBillDate()
    );
  }
}
