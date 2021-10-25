package com.worldpay.pms.bse.domain.model.completebill;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.domain.model.PendingBillLine;
import com.worldpay.pms.bse.domain.model.TestPendingBillLine;
import com.worldpay.pms.bse.domain.model.TestPendingBillLineCalculation;
import com.worldpay.pms.bse.domain.model.TestPendingBillLineServiceQuantity;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

class CompleteBillLineTest {

  @Test
  void testRoundingIsDone() {
    UnaryOperator<BigDecimal> roundingFunction = amount -> amount.setScale(2, RoundingMode.HALF_UP);
    TestPendingBillLineCalculation pendingBillLineCalculation1 = new TestPendingBillLineCalculation("bill_ln_calc_id_1", "class1", "type1",
        BigDecimal.valueOf(10.014), "Y", "rateType", BigDecimal.ONE);
    TestPendingBillLineCalculation pendingBillLineCalculation2 = new TestPendingBillLineCalculation("bill_ln_calc_id_2", "class1", "type1",
        BigDecimal.valueOf(10.015), "Y", "rateType", BigDecimal.ONE);
    TestPendingBillLineServiceQuantity pendingBillLineServiceQuantities = new TestPendingBillLineServiceQuantity("TOTAL_AMT",
        BigDecimal.valueOf(11.0149));
    PendingBillLine pendingBillLine = TestPendingBillLine.builder()
        .billLineId("abc123")
        .pendingBillLineCalculations(new TestPendingBillLineCalculation[]{pendingBillLineCalculation1, pendingBillLineCalculation2})
        .pendingBillLineServiceQuantities(new TestPendingBillLineServiceQuantity[]{pendingBillLineServiceQuantities})
        .build();

    CompleteBillLine completeBillLine = CompleteBillLine.from(pendingBillLine, roundingFunction);

    assertThat(completeBillLine.getTotalAmount().toPlainString(), is("20.03"));
  }
}