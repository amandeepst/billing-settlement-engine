package com.worldpay.pms.bse.domain.account;

import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountDetails;
import com.worldpay.pms.bse.domain.common.LazyStore;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.Function1;
import io.vavr.Tuple2;
import io.vavr.collection.Stream;
import io.vavr.control.Try;
import io.vavr.control.Validation;
import lombok.Builder;
import lombok.NonNull;

public class LazyAccountDetailsStore extends
    LazyStore<String, Validation<DomainError, AccountDetails>> {

  @Builder
  public LazyAccountDetailsStore(@NonNull Integer maximumSize, @NonNull Integer concurrencyLevel,
      @NonNull Integer printStatsInterval,
      @NonNull Function1<String, Validation<DomainError, AccountDetails>> loader) {
    super(maximumSize, concurrencyLevel, printStatsInterval, loader);
  }


  public void preload(Iterable<AccountDetails> accountDetails) {
    super.preload(
        Stream.ofAll(accountDetails).groupBy(AccountDetails::getAccountId)
        .map((id, accountDetails1) -> new Tuple2<>(id,
            AccountDeterminationService.validateAccountDetails(id, accountDetails)))
        .mapValues(Try::success)
        .toJavaMap()
    );
  }
}
