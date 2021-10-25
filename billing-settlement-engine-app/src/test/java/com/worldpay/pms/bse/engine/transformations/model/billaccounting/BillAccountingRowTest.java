package com.worldpay.pms.bse.engine.transformations.model.billaccounting;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.domain.model.billtax.BillLineTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTaxDetail;
import com.worldpay.pms.bse.engine.transformations.model.billprice.BillPriceRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetailRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.HashMap;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class BillAccountingRowTest {

  private static final BillLineServiceQuantityRow BILL_LINE_SVC_QTY = new BillLineServiceQuantityRow("TXN_AMT", BigDecimal.valueOf(60),
      null);

  private static final BillLineCalculationRow BILL_LINE_CALC = new BillLineCalculationRow("billCalcId", "calcLnClass",
      "calcLnType", null, BigDecimal.ONE, "Y", "TXN_AMT", BigDecimal.ONE, "EXE", null,
      "taxDescription");

  private static final BillLineRow BILL_LINE_ROW = new BillLineRow("billLineId", "billLineParty", "PREMIUM", "BVI",
      "productDescription", "EUR", "EUR", BigDecimal.TEN, "EUR", BigDecimal.TEN, BigDecimal.ONE,
      "priceLineId", "merchantCode", BigDecimal.TEN, "EXE", null, new BillLineCalculationRow[]{BILL_LINE_CALC},
      new BillLineServiceQuantityRow[]{BILL_LINE_SVC_QTY});

  private static final BillRow BILL_ROW = new BillRow("billId1", "partyId1", "billSubAccountId1",
      "tariffType", "templateType", "00001", "FUND", "accountId1", "businessUnit", Date.valueOf(LocalDate.now()),
      "MONTHLY", Date.valueOf("2021-01-01"), Date.valueOf("2021-01-31"), "EUR", BigDecimal.TEN, "billRef", "COMPLETE",
      "N", "settSubLevelType", "settSubLevelValue", "granularity", "granularityKeyVal",
      "N", "N", "N", "N", "N", "N", null, null, null, null,
      null, null, null, null, null, null, null, null, null, new BillLineRow[]{BILL_LINE_ROW},
      new BillLineDetailRow[0], new BillPriceRow[0], null, (short) 0);

  private static BillTax billTax;

  @BeforeAll
  static void init() {
    val billLineTax1 = new BillLineTax("EXE", BigDecimal.ZERO, "E Exempt", BigDecimal.TEN, BigDecimal.ZERO);
    val billLineTax2 = new BillLineTax("S", BigDecimal.valueOf(21), "STANDARD-S", BigDecimal.TEN, BigDecimal.valueOf(2.1));
    val billLineTax3 = new BillLineTax("S", BigDecimal.valueOf(21), "STANDARD-S", BigDecimal.valueOf(20), BigDecimal.valueOf(4.2));

    val billLineTaxData = new HashMap<String, BillLineTax>();
    billLineTaxData.put("bill_line_id_1", billLineTax1);
    billLineTaxData.put("bill_line_id_2", billLineTax2);
    billLineTaxData.put("bill_line_id_3", billLineTax3);

    val billTaxId = "tax_id_1";

    val billTaxDetail1 = new BillTaxDetail(billTaxId, "EXE", BigDecimal.ZERO, "E Exempt", BigDecimal.TEN, BigDecimal.ZERO);
    val billTaxDetail2 = new BillTaxDetail(billTaxId, "S", BigDecimal.valueOf(21), "STANDARD-S", BigDecimal.valueOf(30),
        BigDecimal.valueOf(6.3));

    billTax = new BillTax(billTaxId, "bill_id_1", "GB991280207", "wp_reg_1", "VAT", "HMRC", "N", billLineTaxData,
        new BillTaxDetail[]{billTaxDetail1, billTaxDetail2});
  }

  @Test
  void testGetBillAccountingValuesFromBill() {
    val billAccounting = BillAccountingRow.from(BILL_ROW);

    assertThat(billAccounting.length, is(1));
    val bA = billAccounting[0];
    assertThat(bA.getType(), is("BVI"));
    assertThat(bA.getBillClass(), is("BILLING"));
    assertThat(bA.getCalculationLineAmount(), is(BigDecimal.valueOf(1)));
  }

  @Test
  void testGetBillAccountingValuesFromBillAndBillTaxWhereTaxRateIsNotZero() {
    val billAccounting = BillAccountingRow.from(BILL_ROW, billTax);

    assertThat(billAccounting.length, is(1));
    val bA = billAccounting[0];
    assertThat(bA.getType(), is("HMRC-VAT"));
    assertThat(bA.getBillClass(), is("TAX"));
    assertThat(bA.getCalculationLineAmount(), is(BigDecimal.valueOf(6.3)));
  }
}