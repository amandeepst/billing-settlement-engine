package com.worldpay.pms.bse.domain.model.completebill;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.domain.model.PendingBillLineCalculation;
import com.worldpay.pms.bse.domain.model.TestPendingBillLineCalculation;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

class CompleteBillLineCalculationTest {

  @Test
  void testRoundingIsDone() {
    UnaryOperator<BigDecimal> roundingFunction = amount -> amount.setScale(2, RoundingMode.HALF_UP);

    PendingBillLineCalculation pendingBillLineCalc1 = new TestPendingBillLineCalculation("bill_ln_calc_id_1", "class1", "type1",
        BigDecimal.valueOf(12.23501), "Y", "rateType", BigDecimal.ONE);
    PendingBillLineCalculation pendingBillLineCalc2 = new TestPendingBillLineCalculation("bill_ln_calc_id_2", "class1", "type1",
        BigDecimal.valueOf(12.23499), "Y", "rateType", BigDecimal.ONE);

    CompleteBillLineCalculation completeBillLineCalculation1 = CompleteBillLineCalculation.from(pendingBillLineCalc1, roundingFunction);
    CompleteBillLineCalculation completeBillLineCalculation2 = CompleteBillLineCalculation.from(pendingBillLineCalc2, roundingFunction);

    assertThat(completeBillLineCalculation1.getAmount().toPlainString(), is("12.24"));
    assertThat(completeBillLineCalculation2.getAmount().toPlainString(), is("12.23"));
  }
}