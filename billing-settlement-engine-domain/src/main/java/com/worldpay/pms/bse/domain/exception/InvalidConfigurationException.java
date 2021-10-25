package com.worldpay.pms.bse.domain.exception;

/**
 * Exception indicating an error in input configuration.
 */
public class InvalidConfigurationException extends BillingException {

  public InvalidConfigurationException(String message, Object... messageArgs) {
    super(message, messageArgs);
  }

  public InvalidConfigurationException(Exception originalException, String message, Object... messageArgs) {
    super(originalException, message, messageArgs);
  }
}
