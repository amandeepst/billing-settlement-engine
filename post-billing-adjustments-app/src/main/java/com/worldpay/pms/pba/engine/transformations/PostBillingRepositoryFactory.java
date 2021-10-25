package com.worldpay.pms.pba.engine.transformations;

import com.worldpay.pms.pba.engine.data.DefaultPostBillingRepository;
import com.worldpay.pms.pba.engine.data.PostBillingRepository;
import com.worldpay.pms.spark.core.factory.Factory;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import io.vavr.control.Try;
import java.time.LocalDate;

public class PostBillingRepositoryFactory implements Factory<PostBillingRepository> {
  private final JdbcConfiguration conf;
  private LocalDate logicalDate;

  public PostBillingRepositoryFactory(JdbcConfiguration conf, LocalDate logicalDate) {
    this.conf = conf;
    this.logicalDate = logicalDate;
  }

  @Override
  public Try<PostBillingRepository> build() {
    return Try.of(() -> new DefaultPostBillingRepository(conf, logicalDate));
  }
}
