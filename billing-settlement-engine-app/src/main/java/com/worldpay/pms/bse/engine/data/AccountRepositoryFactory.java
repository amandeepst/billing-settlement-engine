package com.worldpay.pms.bse.engine.data;

import com.worldpay.pms.bse.domain.account.AccountRepository;
import com.worldpay.pms.bse.engine.BillingConfiguration.AccountRepositoryConfiguration;
import com.worldpay.pms.spark.core.factory.Factory;
import io.vavr.control.Try;
import java.time.LocalDate;

public class AccountRepositoryFactory implements Factory<AccountRepository> {

  private final AccountRepositoryConfiguration conf;
  private LocalDate logicalDate;

  public AccountRepositoryFactory(AccountRepositoryConfiguration conf,
      LocalDate logicalDate) {
    this.conf = conf;
    this.logicalDate = logicalDate;
  }

  @Override
  public Try<AccountRepository> build() {
    return Try.of(() -> new JdbcAccountRepository(conf, logicalDate));
  }

}
