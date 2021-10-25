package com.worldpay.pms.bse.engine.transformations.model.billprice;

import com.worldpay.pms.bse.domain.account.BillingAccount;
import com.worldpay.pms.bse.domain.model.BillableItemLine;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTaxDetail;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import io.vavr.collection.Array;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.Arrays;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import scala.Tuple2;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class BillPriceRow {

  @NonNull
  String partyId;
  @NonNull
  String accountType;
  @NonNull
  String productId;
  @NonNull
  String calculationLineType;
  @NonNull
  BigDecimal amount;
  @NonNull
  String currency;
  @NonNull
  String billReference;
  @NonNull
  String invoiceableFlag;
  String sourceType;
  String sourceId;
  String billLineDetailId;
  String billableItemId;
  String billId;
  @NonNull
  Date accruedDate;
  String granularity;

  public static BillPriceRow from(Tuple2<BillRow, BillTax> billTax) {
    BillRow bill = billTax._1();
    BillTax tax = billTax._2();

    return new BillPriceRow(
        bill.getPartyId(),
        bill.getAccountType(),
        "RECRTAX",
        "TAX",
        Arrays.stream(tax.getBillTaxDetails()).map(BillTaxDetail::getTaxAmount).reduce(BigDecimal::add).orElse(BigDecimal.ZERO),
        bill.getCurrencyCode(),
        bill.getBillReference(),
        "Y",
        null,
        null,
        null,
        null,
        bill.getBillId(),
        bill.getBillDate(),
        bill.getGranularity()
    );
  }

  public static BillPriceRow[] from(BillRow bill) {
    Array<BillLineRow> billLines = Array.of(bill.getBillLines()).filter(line -> "MINCHRGP".equals(line.getProductId()));

    return billLines.map(billLine ->
        new BillPriceRow(
            billLine.getBillLineParty(),
            bill.getAccountType(),
            billLine.getProductId(),
            billLine.getBillLineCalculations()[0].getCalculationLineType(),
            billLine.getBillLineCalculations()[0].getAmount(),
            bill.getCurrencyCode(),
            bill.getBillReference(),
            "Y",
            null,
            null,
            null,
            null,
            bill.getBillId(),
            bill.getBillDate(),
            bill.getGranularity()
        )).toJavaArray(BillPriceRow[]::new);
  }

  public static BillPriceRow from(BillableItemRow billableItem, BillableItemLine billableItemLine, BillingAccount billingAccount,
      String billReference, String billGranularity) {
    // this is called just for misc billable items

    return new BillPriceRow(
        billingAccount.getPartyId(),
        billingAccount.getAccountType(),
        billableItem.getProductId(),
        billableItemLine.getCalculationLineType(),
        billableItemLine.getAmount(),
        billableItem.getBillingCurrency(),
        billReference,
        "Y",
        billableItem.getSourceType(),
        billableItem.getSourceId(),
        null,
        billableItem.getBillableItemId(),
        null,
        billableItem.getAccruedDate(),
        billGranularity
    );
  }
}