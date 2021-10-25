package com.worldpay.pms.bse.domain.common;

import com.worldpay.pms.pce.common.DomainError;
import java.time.LocalDate;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ErrorCatalog {

  public static final String ACCOUNT_NOT_FOUND = "ACCOUNT_NOT_FOUND";
  public static final String MULTIPLE_ACCOUNTS_FOUND = "MULTIPLE_ACCOUNTS_FOUND";
  public static final String NO_BILLING_CYCLE = "NO_BILLING_CYCLE";
  public static final String ACCOUNT_DETAILS_NOT_FOUND = "ACCOUNT_DETAILS_NOT_FOUND";
  public static final String MULTIPLE_ACCOUNT_DETAILS_ENTRIES_FOUND = "MULTIPLE_ACCOUNT_DETAILS_ENTRIES_FOUND";
  public static final String NO_TAX_RULE_FOUND = "NO_TAX_RULE_FOUND";
  public static final String NO_TAX_PRODUCT_CHARACTERISTIC_FOUND = "NO_TAX_PRODUCT_CHARACTERISTIC_FOUND";
  public static final String NO_TAX_RATE_FOUND = "NO_TAX_RATE_FOUND";
  public static final String LINE_CALC_MISCALCULATION_DETECTED = "LINE_CALC_MISCALCULATION_DETECTED";
  public static final String NO_BILL_FOUND_FOR_CORRECTION_ID = "NO_BILL_FOUND_FOR_CORRECTION_ID";
  public static final String INVALID_CORRECTION = "INVALID_CORRECTION";

  public static final String NULL = "NULL";
  public static final String NULL_OR_EMPTY = "NULL_OR_EMPTY";

  public static DomainError accountNotFound(String accountKey) {
    return DomainError.of(ACCOUNT_NOT_FOUND, "Could not determine account details for key %s", accountKey);
  }

  public static DomainError multipleAccountsFound(String accountKey) {
    return DomainError.of(MULTIPLE_ACCOUNTS_FOUND, "Found multiple entries for account details for key %s", accountKey);
  }

  public static DomainError noBillingCycle(String accountKey) {
    return DomainError.of(NO_BILLING_CYCLE, "The account with key %s has no billing cycle", accountKey);
  }

  public static DomainError accountDetailsNotFound(String accountId) {
    return DomainError.of(ACCOUNT_DETAILS_NOT_FOUND,
        "Could not determine processing group and billing cycle code for account id %S", accountId);
  }

  public static DomainError multipleAccountDetailsEntriesFound(String accountId) {
    return DomainError.of(MULTIPLE_ACCOUNT_DETAILS_ENTRIES_FOUND,
        "Multiple rows found when trying to get processing group and billing cycle code for account id %S", accountId);
  }

  public static DomainError noBillingCycleFound(String billingCycleCode, LocalDate date) {
    return DomainError.of(NO_BILLING_CYCLE, "No bill cycle found for code %s and date %s", billingCycleCode, date);
  }

  public static DomainError noTaxRuleFound(String key) {
    return DomainError.of(NO_TAX_RULE_FOUND, "No tax rule found for key `[%s]`", key);
  }

  public static DomainError noTaxProductCharacteristicFound(String productCode, String characteristicType) {
    return DomainError.of(NO_TAX_PRODUCT_CHARACTERISTIC_FOUND,
        "No tax product characteristic found for key `[%s, %s]`", productCode, characteristicType);
  }

  public static DomainError noTaxRateFound(String taxStatusType, String taxStatusValue) {
    return DomainError.of(NO_TAX_RATE_FOUND, "No tax rate found for key `[%s, %s]`", taxStatusType, taxStatusValue);
  }

  public static DomainError lineCalcMiscalculationDetected() {
    return DomainError
        .of(LINE_CALC_MISCALCULATION_DETECTED, "Bill Line Calc miscalculation detected on bill. Check Splunk for billable item ids.");
  }

  public static DomainError noBillFoundForCorrection(String billId, String correctionEventId) {
    return DomainError
        .of(NO_BILL_FOUND_FOR_CORRECTION_ID, "No bill found with id [%s] suitable for correction id [%s]", billId, correctionEventId);
  }

  public static DomainError invalidCorrection(String billId, String correctionEventId, String errorMessage) {
    return DomainError
        .of(INVALID_CORRECTION, "Invalid correction for bill id [%s] and correction id [%s] with error: [%s]", billId,
            correctionEventId, errorMessage);
  }

  //
  // generic
  //
  public static DomainError unexpectedNull(String field) {
    return DomainError.of(NULL, "Unexpected null field `%s`", field.toUpperCase());
  }

  public static DomainError unexpectedNullOrEmpty(String field) {
    return DomainError.of(NULL_OR_EMPTY, "Unexpected null or empty field `%s`", field.toUpperCase());
  }

}
