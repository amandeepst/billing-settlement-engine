package com.worldpay.pms.bse.engine.transformations;

import com.worldpay.pms.bse.domain.account.AccountDeterminationService;
import com.worldpay.pms.bse.domain.account.AccountRepository;
import com.worldpay.pms.bse.domain.account.DefaultAccountDeterminationService;
import com.worldpay.pms.bse.engine.BillingConfiguration.Billing;
import com.worldpay.pms.spark.core.factory.Factory;
import io.vavr.control.Try;

public class AccountDeterminationServiceFactory implements Factory<AccountDeterminationService> {

  private final Factory<AccountRepository> accountRepositoryFactory;
  private final Billing billingConfig;

  public AccountDeterminationServiceFactory(Factory<AccountRepository> accountRepositoryFactory,
      Billing billingConfig) {
    this.accountRepositoryFactory = accountRepositoryFactory;
    this.billingConfig = billingConfig;
  }
  @Override
  public Try<AccountDeterminationService> build() {
    return Try.of(() -> new DefaultAccountDeterminationService(
        billingConfig.getAccountsListMaxEntries(),
        billingConfig.getAccountsListConcurrency(),
        billingConfig.getAccountsListStatsInterval(),
        accountRepositoryFactory.build().get()
    ));
  }
}