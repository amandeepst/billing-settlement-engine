package com.worldpay.pms.bse.domain.model.completebill;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.domain.model.PendingBillLineServiceQuantity;
import com.worldpay.pms.bse.domain.model.TestPendingBillLineServiceQuantity;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

class CompleteBillLineServiceQuantityTest {

  @Test
  void testRoundingIsDone() {
    UnaryOperator<BigDecimal> roundingFunction = amount -> amount.setScale(2, RoundingMode.HALF_UP);

    PendingBillLineServiceQuantity pendingBillLineSvcQty1 = new TestPendingBillLineServiceQuantity("F_M_AMT", BigDecimal.valueOf(99.9999));
    PendingBillLineServiceQuantity pendingBillLineSvcQty2 = new TestPendingBillLineServiceQuantity("TOTAL_AMT",
        BigDecimal.valueOf(100.891));

    CompleteBillLineServiceQuantity completeBillLineServiceQuantity1 = CompleteBillLineServiceQuantity
        .from(pendingBillLineSvcQty1, roundingFunction);
    CompleteBillLineServiceQuantity completeBillLineServiceQuantity2 = CompleteBillLineServiceQuantity
        .from(pendingBillLineSvcQty2, roundingFunction);

    assertThat(completeBillLineServiceQuantity1.getServiceQuantityValue().toPlainString(), is("99.9999"));
    assertThat(completeBillLineServiceQuantity2.getServiceQuantityValue().toPlainString(), is("100.89"));
  }
}