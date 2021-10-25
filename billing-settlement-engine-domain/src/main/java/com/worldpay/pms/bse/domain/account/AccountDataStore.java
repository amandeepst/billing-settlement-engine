package com.worldpay.pms.bse.domain.account;

import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountKey;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.control.Validation;

public interface AccountDataStore {

  Validation<DomainError, BillingAccount> get(AccountKey key);
}
