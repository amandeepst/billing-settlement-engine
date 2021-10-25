package com.worldpay.pms.bse.domain.validation;

import com.worldpay.pms.bse.domain.account.BillingAccount;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;


@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class ValidationResult {

  BillingAccount billingAccount;

  boolean isCalculationCorrect;

}
