package com.worldpay.pms.bse.domain.validation;

import static com.worldpay.pms.bse.domain.validation.TestBillableItem.getStandardBillableItem;
import static com.worldpay.pms.bse.domain.validation.TestBillableItemLine.getBigAmountBillableItemLines;
import static com.worldpay.pms.bse.domain.validation.TestBillableItemLine.getBillableItemLineWithNullCalcLnType;
import static com.worldpay.pms.bse.domain.validation.TestBillableItemLine.getBillableItemLines;
import static com.worldpay.pms.bse.domain.validation.TestBillableItemServiceQuantity.getBadAlternatePricingFxSvcQty;
import static com.worldpay.pms.bse.domain.validation.TestBillableItemServiceQuantity.getBadChargingFxSvcQty;
import static com.worldpay.pms.bse.domain.validation.TestBillableItemServiceQuantity.getBadFundingAmountSvcQty;
import static com.worldpay.pms.bse.domain.validation.TestBillableItemServiceQuantity.getBadFundingFxSvcQty;
import static com.worldpay.pms.bse.domain.validation.TestBillableItemServiceQuantity.getCorrectBigAmountSvcQty;
import static com.worldpay.pms.bse.domain.validation.TestBillableItemServiceQuantity.getCorrectSvcQty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.domain.model.BillableItem;
import org.junit.jupiter.api.Test;

public class DefaultCalculationCheckServiceTest {


  private static final CalculationCheckService calculationCheckService = new DefaultCalculationCheckService();

  @Test
  void whenMisingFundingRateThenReturnFalse() {
    BillableItem billableItem = getStandardBillableItem(getBadFundingFxSvcQty(), getBillableItemLines());
    boolean isCalculationCorrect = calculationCheckService.apply(billableItem);

    assertThat(isCalculationCorrect, is(false));
  }

  @Test
  void whenMissingChargingRateThenReturnFalse() {
    BillableItem billableItem = getStandardBillableItem(getBadChargingFxSvcQty(), getBillableItemLines());
    boolean isCalculationCorrect = calculationCheckService.apply(billableItem);

    assertThat(isCalculationCorrect, is(false));
  }

  @Test
  void whenAlternatePricingFxRateMissingThenReturnFalse() {
    BillableItem billableItem = getStandardBillableItem(getBadAlternatePricingFxSvcQty(), getBillableItemLines());
    boolean isCalculationCorrect = calculationCheckService.apply(billableItem);

    assertThat(isCalculationCorrect, is(false));
  }

  @Test
  void whenFundingAmountIsWrongThenReturnFalse() {
    BillableItem billableItem = getStandardBillableItem(getBadFundingAmountSvcQty(), getBillableItemLines());
    boolean isCalculationCorrect = calculationCheckService.apply(billableItem);

    assertThat(isCalculationCorrect, is(false));
  }

  @Test
  void whenCalculationIsCorrectThenReturnTrue() {
    BillableItem billableItem = getStandardBillableItem(getCorrectSvcQty(), getBillableItemLines());
    boolean isCalculationCorrect = calculationCheckService.apply(billableItem);

    assertThat(isCalculationCorrect, is(true));
  }

  @Test
  void whenBigCalculationAmountIsCorrectThenReturnTrue() {
    BillableItem billableItem = getStandardBillableItem(getCorrectBigAmountSvcQty(), getBigAmountBillableItemLines());
    boolean isCalculationCorrect = calculationCheckService.apply(billableItem);

    assertThat(isCalculationCorrect, is(true));
  }

  @Test
  void whenCalculationLineTypeIsNullThenReturnTrue() {
    BillableItem billableItem = getStandardBillableItem(getCorrectSvcQty(), getBillableItemLineWithNullCalcLnType());
    boolean isCalculationCorrect = calculationCheckService.apply(billableItem);

    assertThat(isCalculationCorrect, is(true));
  }


}
