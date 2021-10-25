package com.worldpay.pms.bse.engine.runresult;

import static io.vavr.control.Validation.invalid;
import static io.vavr.control.Validation.valid;
import static java.lang.String.format;

import com.worldpay.pms.bse.engine.BillingBatchRunResult;
import com.worldpay.pms.spark.core.batch.BatchRunErrorPolicy.PercentThresholdBatchRunErrorPolicy;
import com.worldpay.pms.spark.core.batch.BatchRunResult;
import io.vavr.control.Validation;
import java.math.BigInteger;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.commons.math3.fraction.BigFraction;

@EqualsAndHashCode(callSuper = true)
@Value
public class BillingPercentThresholdPolicy extends PercentThresholdBatchRunErrorPolicy {
  private static final double ZERO_PERCENT = 0.0D;
  private static final double ONE_HUNDRED_PERCENT = 100.0D;

  String resultType;

  public BillingPercentThresholdPolicy(PercentThreshold threshold, String resultType) {
    super(threshold);
    this.resultType = resultType;
  }

  @Override
  public <T extends BatchRunResult> Validation<String, T> check(T batchRunResult) {
    ResultCount resultCount = ((BillingBatchRunResult)batchRunResult).getResultCount(resultType);
    long errorCheckedCount = resultCount.getFailedTodayCount();
    long totalCheckedCount = resultCount.getSuccessCount() + resultCount.getFailedTodayCount();
    double errorRate = errorRate(resultCount.getSuccessCount(), errorCheckedCount);
    if(!threshold.isDisabled() && errorRate > threshold.getValue()) {
      return invalid(
          format(
              "Driver interrupted, error '%s' exceed percent threshold of '%s%%', with '%s' errors from '%s' total",
              resultType, threshold.getValue(), errorCheckedCount, totalCheckedCount
          )
      );
    }
    return valid(batchRunResult);
  }

  private double errorRate(long successCheckCount, long errorCheckedCount) {
    long totalCheckedCount = successCheckCount + errorCheckedCount;
    if (totalCheckedCount == 0L) {
      return ZERO_PERCENT;
    } else {
      return successCheckCount == 0L ? ONE_HUNDRED_PERCENT :
             (new BigFraction(BigInteger.valueOf(errorCheckedCount),
                              BigInteger.valueOf(totalCheckedCount))).percentageValue();
    }
  }

}
