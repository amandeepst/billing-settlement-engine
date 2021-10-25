package com.worldpay.pms.bse.engine.runresult;

import static io.vavr.control.Validation.invalid;
import static io.vavr.control.Validation.valid;
import static java.lang.String.format;

import com.worldpay.pms.bse.engine.BillingBatchRunResult;
import com.worldpay.pms.spark.core.batch.BatchRunErrorPolicy.AbsoluteThresholdBatchRunErrorPolicy;
import com.worldpay.pms.spark.core.batch.BatchRunResult;
import io.vavr.control.Validation;
import lombok.EqualsAndHashCode;
import lombok.Value;

@EqualsAndHashCode(callSuper = true)
@Value
public class BillingAbsoluteThresholdPolicy extends AbsoluteThresholdBatchRunErrorPolicy {
  String resultType;

  public BillingAbsoluteThresholdPolicy(long threshold, String resultType) {
    super(threshold>=0? threshold:Long.MAX_VALUE);
    this.resultType = resultType;
  }

  @Override
  public <T extends BatchRunResult> Validation<String, T> check(T result) {
    ResultCount resultCount = ((BillingBatchRunResult)result).getResultCount(resultType);
    long errorCheckedCount = resultCount.getFailedTodayCount();
    long totalCheckedCount = resultCount.getSuccessCount() + resultCount.getFailedTodayCount();
    if(errorCheckedCount >= threshold) {
      return invalid(
          format(
              "Driver interrupted, error '%s' exceed absolute threshold of '%s', with '%s' errors from '%s' total",
              resultType, threshold, errorCheckedCount, totalCheckedCount
          )
      );
    }
    return valid(result);
  }
}
