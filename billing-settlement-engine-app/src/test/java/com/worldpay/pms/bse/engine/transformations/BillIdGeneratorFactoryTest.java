package com.worldpay.pms.bse.engine.transformations;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.worldpay.pms.bse.engine.BillingConfiguration;
import com.worldpay.pms.bse.engine.BillingConfiguration.BillIdSeqConfiguration;
import com.worldpay.pms.bse.engine.transformations.model.IdGenerator;
import com.worldpay.pms.bse.engine.utils.WithDatabase;
import com.worldpay.pms.spark.core.factory.ExecutorLocalFactory;
import lombok.val;
import org.junit.jupiter.api.Test;

class BillIdGeneratorFactoryTest implements WithDatabase {

  private BillIdSeqConfiguration conf;
  @Override
  public void bindBillingConfiguration(BillingConfiguration conf) {
   this.conf = conf.getBillIdSeqConfiguration();
  }

  @Test
  public void checkExecutorLocalFactoryBuildAlwaysReturnsTheSameServiceInstance() {
    val billIdGeneratorFactory  = ExecutorLocalFactory.of(new BillIdGeneratorFactory(conf));

    IdGenerator idGenerator = billIdGeneratorFactory.build().get();

    assertThat(idGenerator, is(notNullValue()));
    assertThat(billIdGeneratorFactory.build().get(), is(idGenerator));
  }
}