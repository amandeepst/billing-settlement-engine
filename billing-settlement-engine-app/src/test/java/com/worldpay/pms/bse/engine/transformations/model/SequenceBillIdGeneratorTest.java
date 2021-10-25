package com.worldpay.pms.bse.engine.transformations.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.engine.BillingConfiguration;
import com.worldpay.pms.bse.engine.BillingConfiguration.BillIdSeqConfiguration;
import com.worldpay.pms.bse.engine.utils.WithDatabase;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.sql2o.Sql2oQuery;

class SequenceBillIdGeneratorTest implements WithDatabase {

  private SqlSequenceBasedIdGenerator billIdGenerator;

  private SqlDb billingDb;

  @Override
  public void bindBillingConfiguration(BillingConfiguration conf) {
    BillIdSeqConfiguration seqConfiguration = conf.getBillIdSeqConfiguration();
    this.billIdGenerator = new SqlSequenceBasedIdGenerator(seqConfiguration.getBillIdSequenceName(), seqConfiguration.getBillIdIncrementBy(), billingDb);
  }

  @Override
  public void bindBillingJdbcConfiguration(JdbcConfiguration conf) {
    this.billingDb = SqlDb.simple(conf);
  }

  @Test
  void whenGetBillIdThenReturnCorrectValues() {
    billingDb.execQuery("update-bill-id-sq",
        "ALTER SEQUENCE cbe_bse_owner.BILL_ID_SQ RESTART START WITH 1000000000001", Sql2oQuery::executeUpdate);
    assertThat(billIdGenerator.generateId(), is(equalTo("1000000000001")));
    assertThat(billIdGenerator.generateId(), is(equalTo("1000000000002")));

    IntStream.range(2, 1000).forEach(i -> billIdGenerator.generateId());

    //We need the alter sequence because of the sequence cache. It might return different values.
    billingDb.execQuery("update-bill-id-sq",
        "ALTER SEQUENCE cbe_bse_owner.BILL_ID_SQ RESTART START WITH 1000000000001", Sql2oQuery::executeUpdate);

    assertThat(billIdGenerator.generateId(), is(("1000000000001")));
  }
}