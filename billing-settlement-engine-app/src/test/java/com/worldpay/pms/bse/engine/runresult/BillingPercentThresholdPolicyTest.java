package com.worldpay.pms.bse.engine.runresult;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.worldpay.pms.bse.engine.BillingBatchRunResult;
import com.worldpay.pms.spark.core.batch.BatchRunErrorPolicy.PercentThreshold;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class BillingPercentThresholdPolicyTest {

  private static final BillingPercentThresholdPolicy BILLING_TEN_PERCENT_THRESHOLD_POLICY = new BillingPercentThresholdPolicy(
      PercentThreshold.of(10.0D), BillingBatchRunResult.BILLS_RESULT_TYPE);

  @ParameterizedTest
  @CsvSource({"0, 10, 100, 10, 0",
      "0, 9, 100, 9, 0",
      "0, 0, 100, 0, 0",
      "0, 0, 0, 0, 0",
      "10, 19, 100, 9, 0",
      "80, 89, 189, 9, 0",
      "90, 99, 199, 9, 0",
      "2, 2, 15, 0, 0"})
  void validIfErrorRateIsEqualToOrLessThanThreshold(int ignoredCount, int errorCount, int totalCount, int failedTodayCount,
      int fixedCount) {
    ResultCount testedResult = new ResultCount(ignoredCount, errorCount, totalCount, failedTodayCount, fixedCount);
    BillingBatchRunResult billingBatchRunResult = BillingBatchRunResult.builder().billResultCount(testedResult).build();
    val validation = BILLING_TEN_PERCENT_THRESHOLD_POLICY.check(
        billingBatchRunResult
    );
    assertThat(validation.isValid(), is(true));
  }

  @ParameterizedTest
  @CsvSource({"0, 100, 100, 100, 0",
      "0, 20, 100, 20, 0",
      "80, 89, 100, 9, 0",
      "50, 89, 100, 39, 0"})
  void invalidIfErrorRateIsBreached(int ignoredCount, int errorCount, int totalCount, int failedTodayCount, int fixedCount) {
    ResultCount testedResult = new ResultCount(ignoredCount, errorCount, totalCount, failedTodayCount, fixedCount);
    BillingBatchRunResult billingBatchRunResult = BillingBatchRunResult.builder().billResultCount(testedResult).build();
    val validation = BILLING_TEN_PERCENT_THRESHOLD_POLICY.check(
        billingBatchRunResult
    );
    assertThat(validation.isValid(), is(false));
    assertThat(validation.getError(),
        is("Driver interrupted, error 'bills' exceed percent threshold of '10.0%', with '" + failedTodayCount + "' errors from '"
            + (totalCount - ignoredCount) + "' total"));
  }

  @Test
  void doesNotThrowErrorIfErrorRateZero() {
    ResultCount testedResult = new ResultCount(0, 0, 0, 0, 0);
    BillingBatchRunResult billingBatchRunResult = BillingBatchRunResult.builder().billResultCount(testedResult).build();
    assertDoesNotThrow(() -> BILLING_TEN_PERCENT_THRESHOLD_POLICY.check(
        billingBatchRunResult
    ));
  }

  @Test
  void validIfThresholdIsDisabled() {
    BillingPercentThresholdPolicy disabledThresholdPolicy =
        new BillingPercentThresholdPolicy(PercentThreshold.of(-1.0D), BillingBatchRunResult.BILLS_RESULT_TYPE);
    ResultCount testedResult = new ResultCount(0, 5, 100, 5, 0);
    BillingBatchRunResult billingBatchRunResult = BillingBatchRunResult.builder().billResultCount(testedResult).build();
    val validation = disabledThresholdPolicy.check(
        billingBatchRunResult
    );
    assertThat(validation.isValid(), is(true));
  }
}