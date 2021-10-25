package com.worldpay.pms.bse.engine;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.worldpay.pms.bse.domain.exception.BillingException;
import com.worldpay.pms.bse.engine.runresult.ResultCount;
import com.worldpay.pms.spark.core.batch.BatchRunResult;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class BillingBatchRunResult implements BatchRunResult {

  public static final String BILLS_RESULT_TYPE = "bills";
  public static final String BILLABLE_ITEMS_RESULT_TYPE = "billable items";

  ResultCount billableItemResultCount;

  ResultCount billResultCount;

  long runId;

  long pendingBillCount;

  long billLineDetailCount;
  long pendingMinimumChargeCount;
  long minChargeBillsCount;
  long billPriceCount;
  long billTaxCount;
  long billAccountingCount;
  long billRelationshipCount;

  @JsonIgnore
  @Override
  public long getErrorTransactionsCount() {
    return billableItemResultCount.getErrorCount() + billResultCount.getErrorCount();
  }

  @JsonIgnore
  @Override
  public long getSuccessTransactionsCount() {
    return billableItemResultCount.getSuccessCount() + billResultCount.getSuccessCount();
  }

  public ResultCount getResultCount(String resultType) {
    switch (resultType) {
      case BILLS_RESULT_TYPE:
        return getBillResultCount();
      case BILLABLE_ITEMS_RESULT_TYPE:
        return getBillableItemResultCount();
      default:
        throw new BillingException("Unrecognized run result type " + resultType);
    }
  }
}
