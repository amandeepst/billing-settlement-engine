package com.worldpay.pms.bse.engine.data;

import com.worldpay.pms.bse.domain.account.AccountRepository;
import com.worldpay.pms.bse.engine.BillingConfiguration.StaticDataRepositoryConfiguration;
import com.worldpay.pms.spark.core.factory.Factory;
import io.vavr.control.Try;
import java.sql.Date;

public class BillingRepositoryFactory implements Factory<BillingRepository> {

  private final Date logicalDate;
  private final StaticDataRepositoryConfiguration conf;
  private final Factory<AccountRepository> accountRepositoryFactory;

  public BillingRepositoryFactory(StaticDataRepositoryConfiguration conf, Date logicalDate,
      Factory<AccountRepository> accountRepositoryFactory) {
    this.logicalDate = logicalDate;
    this.conf = conf;
    this.accountRepositoryFactory = accountRepositoryFactory;
  }

  @Override
  public Try<BillingRepository> build() {
    return Try.of(() -> new DefaultBillingRepository(
        logicalDate, new JdbcStaticDataRepository(conf.getConf(), logicalDate),
        new CsvStaticDataRepository(logicalDate),
        accountRepositoryFactory.build().get()));
  }
}