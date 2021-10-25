package com.worldpay.pms.bse.engine.transformations;

import com.worldpay.pms.bse.engine.BillingConfiguration.BillIdSeqConfiguration;
import com.worldpay.pms.bse.engine.transformations.model.IdGenerator;
import com.worldpay.pms.bse.engine.transformations.model.SqlSequenceBasedIdGenerator;
import com.worldpay.pms.spark.core.factory.Factory;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import io.vavr.control.Try;

public class BillIdGeneratorFactory implements Factory<IdGenerator> {

  private final BillIdSeqConfiguration configuration;

  public BillIdGeneratorFactory(BillIdSeqConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public Try<IdGenerator> build() {
    return Try.of(() -> new SqlSequenceBasedIdGenerator(configuration.getBillIdSequenceName(), configuration.getBillIdIncrementBy(),
        SqlDb.simple(configuration.getDataSource())));
  }
}
