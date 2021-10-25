package com.worldpay.pms.bse.engine.transformations;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.worldpay.pms.bse.domain.account.AccountDeterminationService;
import com.worldpay.pms.bse.domain.account.AccountRepository;
import com.worldpay.pms.bse.engine.BillingConfiguration;
import com.worldpay.pms.bse.engine.BillingConfiguration.Billing;
import com.worldpay.pms.bse.engine.DummyFactory;
import com.worldpay.pms.bse.engine.InMemoryAccountRepository;
import com.worldpay.pms.spark.core.factory.ExecutorLocalFactory;
import lombok.val;
import org.junit.jupiter.api.Test;

class AccountDeterminationServiceFactoryTest {

  @Test
  public void checkExecutorLocalFactoryBuildAlwaysReturnsTheSameServiceInstance() {
    BillingConfiguration.Billing billingConfiguration = new Billing();

    val accountRepositoryFactory = new DummyFactory<AccountRepository>(InMemoryAccountRepository.builder().build());
    val accountDeterminationServiceFactory = ExecutorLocalFactory.of(
        new AccountDeterminationServiceFactory(accountRepositoryFactory, billingConfiguration));

    AccountDeterminationService accountDeterminationService = accountDeterminationServiceFactory.build().get();

    assertThat(accountDeterminationService, is(notNullValue()));
    assertThat(accountDeterminationServiceFactory.build().get(), is(accountDeterminationService));
  }
}