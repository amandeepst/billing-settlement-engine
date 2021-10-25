package com.worldpay.pms.pba.engine;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.worldpay.pms.spark.core.batch.BatchRunResult;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PostBillingBatchRunResult implements BatchRunResult {

  long billCount;
  long adjustmentCount;
  long adjustmentAccountingCount;

  @JsonIgnore
  @Override
  public long getErrorTransactionsCount() {
    return 0;
  }

  @JsonIgnore
  @Override
  public long getSuccessTransactionsCount() {
    return 0;
  }
}
