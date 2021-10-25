package com.worldpay.pms.bse.engine.transformations.billcorrection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.worldpay.pms.bse.domain.model.BillLineCalculation;
import com.worldpay.pms.bse.domain.model.billtax.BillLineTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTaxDetail;
import com.worldpay.pms.bse.engine.domain.billcorrection.BillCorrectionService;
import com.worldpay.pms.bse.engine.transformations.model.billprice.BillPriceRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetailRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

class BillCorrectionServiceTest {

  private static BillLineCalculationRow calc1;
  private static BillLineCalculationRow calc2;
  private static BillLineCalculationRow calc3;
  private static BillLineRow line1;
  private static BillRow billRow;

  private static BillCorrectionService billCorrectionService;

  @BeforeAll
  static void init() {
    val svcQty1 = new BillLineServiceQuantityRow("TXN_VOL", BigDecimal.valueOf(1), null);
    val svcQty2 = new BillLineServiceQuantityRow("F_M_AMT", BigDecimal.valueOf(1), null);

    calc1 = new BillLineCalculationRow("calc1", "FND-FX-RES", null, null, BigDecimal.valueOf(83), "N",
        "NA", BigDecimal.ONE, null, null, null);
    calc2 = new BillLineCalculationRow("calc2", "FND-FX-INC", null, null, BigDecimal.valueOf(-83), "N",
        "MSC_PI", BigDecimal.ONE, null, null, null);
    calc3 = new BillLineCalculationRow("calc3", "SETT-CTL", "FND_AMTM", "Funding Merchant Amount",
        BigDecimal.valueOf(-2238), "Y", "MSC_PI", BigDecimal.ONE, null, null, null);

    line1 = new BillLineRow("line1", "addd8784-1d64-40ae-9a66-5d279a11cff7", "CHARGEBACK", "FCBVI",
        "Funding - Purchase - Visa", "GBP", "JPY", BigDecimal.ZERO, "BRL", BigDecimal.valueOf(-113), BigDecimal.ONE, "9212918001", null,
        BigDecimal.valueOf(-2238), null, null, new BillLineCalculationRow[]{calc1, calc2, calc3},
        new BillLineServiceQuantityRow[]{svcQty1, svcQty2});

    billRow = new BillRow("0052df30-1fb6-4268-ae64-c8fbfcbb6f0a", "PO4000135550", "3300442505", null, null, "PO1100000001", "CHRG",
        "5805807403", "PO1300000002", Date.valueOf(LocalDate.now()), "WPDY", Date.valueOf("2021-01-01"), Date.valueOf("2021-01-31"), "GBP",
        BigDecimal.valueOf(-2238), "33004429582021-05-11NNNN", "COMPLETED", "N", null, null, "2105113300442958258839213", null, "N", "N",
        "N", "N", "N", "N", null, null, null, null, null, null, null, null, null, null, null, null, null, new BillLineRow[]{line1},
        new BillLineDetailRow[0], new BillPriceRow[0], null, (short) 0);

    billCorrectionService = new BillCorrectionService();
  }

  @Test
  void testCorrectBill() {
    val correctedBill = new BillCorrectionService().correctBill(billRow, "1000000000001");

    assertThat(correctedBill.getBillAmount(), is(billRow.getBillAmount().multiply(BigDecimal.ONE.negate())));
    assertThat(correctedBill.getPreviousBillId(), is(billRow.getBillId()));

    val correctedBillLines = Arrays.stream(correctedBill.getBillLines()).collect(Collectors.toList());
    assertThat(correctedBillLines.size(), is(1));
    val correctedLine = correctedBillLines.get(0);
    assertThat(correctedLine.getTotalAmount(), is(line1.getTotalAmount().negate()));
    assertThat(correctedLine.getPreviousBillLine(), is(line1.getBillLineId()));

    val correctedCalcAmounts = Arrays.stream(correctedLine.getBillLineCalculations()).map(BillLineCalculation::getAmount)
        .collect(Collectors.toList());
    assertThat(calc1.getAmount().negate(), in(correctedCalcAmounts));
    assertThat(calc2.getAmount().negate(), in(correctedCalcAmounts));
    assertThat(calc3.getAmount().negate(), in(correctedCalcAmounts));

    val correctedServiceQuantities = Arrays.stream(correctedLine.getBillLineServiceQuantities())
        .map(x -> Tuple2.apply(x.getServiceQuantityCode(), x.getServiceQuantity())).collect(Collectors.toList());
    assertThat(correctedServiceQuantities.size(), is(2));
    val tSvcQty1 = correctedServiceQuantities.get(0);
    val tSvcQty2 = correctedServiceQuantities.get(1);

    if (tSvcQty1._1.equals("F_M_AMT")) {
      assertThat(tSvcQty1._2, is(BigDecimal.valueOf(-1)));
    } else {
      assertThat(tSvcQty1._2, is(BigDecimal.ONE));
    }

    if (tSvcQty2._1.equals("F_M_AMT")) {
      assertThat(tSvcQty2._2, is(BigDecimal.valueOf(-1)));
    } else {
      assertThat(tSvcQty2._2, is(BigDecimal.ONE));
    }
  }

  @Test
  void testCorrectBillTax() {
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

    val billTax = new BillTax(billTaxId, "bill_id_1", "GB991280207", "wp_reg_1", "VAT", "HMRC", "N", billLineTaxData,
        new BillTaxDetail[]{billTaxDetail1, billTaxDetail2});

    val correctedBillId = "corrected_bill_id_1";

    val correctedBillTax = billCorrectionService.correctBillTax(correctedBillId, billTax);

    assertThat(correctedBillTax.getBillId(), is(correctedBillId));
    val allTaxBillDetailsAreNegated = Arrays.stream(correctedBillTax.getBillTaxDetails())
        .allMatch(bTD -> bTD.getTaxAmount().compareTo(BigDecimal.ZERO) <= 0 && bTD.getNetAmount().compareTo(BigDecimal.ZERO) <= 0);
    assertTrue(allTaxBillDetailsAreNegated);
  }

}