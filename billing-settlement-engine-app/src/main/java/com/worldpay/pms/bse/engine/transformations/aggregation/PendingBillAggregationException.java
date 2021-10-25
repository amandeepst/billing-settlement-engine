package com.worldpay.pms.bse.engine.transformations.aggregation;

import com.worldpay.pms.bse.domain.exception.BillingException;

public class PendingBillAggregationException extends BillingException {

  public PendingBillAggregationException(String message, Object... messageArgs) {
    super(message, messageArgs);
  }

  public PendingBillAggregationException(Exception originalException, String message, Object... messageArgs) {
    super(originalException, message, messageArgs);
  }
}
