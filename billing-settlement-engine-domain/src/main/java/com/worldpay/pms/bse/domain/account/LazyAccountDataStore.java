package com.worldpay.pms.bse.domain.account;

import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountKey;
import com.worldpay.pms.bse.domain.common.LazyStore;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.Function1;
import io.vavr.control.Validation;
import lombok.Builder;
import lombok.NonNull;

public class LazyAccountDataStore extends
    LazyStore<AccountKey, Validation<DomainError, BillingAccount>>
    implements AccountDataStore {

  @Builder
  public LazyAccountDataStore(@NonNull Integer maximumSize, @NonNull Integer concurrencyLevel,
      @NonNull Integer printStatsInterval,
      @NonNull Function1<AccountKey, Validation<DomainError, BillingAccount>> loader) {
    super(maximumSize, concurrencyLevel, printStatsInterval, loader);
  }

}
