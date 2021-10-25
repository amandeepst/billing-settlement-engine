package com.worldpay.pms.bse.domain.exception;

import com.worldpay.pms.spark.core.PMSException;

public class BillingException extends PMSException {

  public BillingException(String message, Object... messageArgs) {
    super(messageArgs.length > 0 ? String.format(message, messageArgs) : message);
  }

  public BillingException(Exception originalException, String message, Object... messageArgs) {
    super(messageArgs.length > 0 ? String.format(message, messageArgs) : message, originalException);
  }
}
