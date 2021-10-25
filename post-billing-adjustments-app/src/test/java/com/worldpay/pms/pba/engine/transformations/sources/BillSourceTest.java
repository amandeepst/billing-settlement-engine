package com.worldpay.pms.pba.engine.transformations.sources;

import static com.worldpay.pms.testing.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static com.worldpay.pms.testing.utils.SourceTestUtil.insertBatchHistoryAndOnBatchCompleted;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.worldpay.pms.pba.engine.PostBillingConfiguration;
import com.worldpay.pms.pba.engine.model.input.BillRow;
import com.worldpay.pms.pba.engine.utils.WithDatabaseAndSpark;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.JdbcSourceConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.utils.DbUtils;
import java.sql.Date;
import java.sql.Timestamp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BillSourceTest implements WithDatabaseAndSpark {

  private SqlDb billingDb;
  private BillSource billSource;

  @Override
  public void bindPostBillingConfiguration(PostBillingConfiguration conf) {
    JdbcSourceConfiguration billConfiguration = conf.getSources().getBill();
    this.billSource = new BillSource(billConfiguration);
  }

  @Override
  public void bindPostBillingJdbcConfiguration(JdbcConfiguration conf) {
    this.billingDb = SqlDb.simple(conf);
  }

  @BeforeEach
  public void cleanUp() {
    DbUtils.cleanUp(billingDb, "bill");
  }

  @Test
  void testEmptyInput(SparkSession spark) {
    Batch batch = insertBatchHistoryAndOnBatchCompleted(billingDb, "empty_test", "COMPLETED", Timestamp.valueOf("2021-09-01 00:00:00"),
        Timestamp.valueOf("2021-10-10 00:00:00"), new Timestamp(System.currentTimeMillis()), Timestamp.valueOf("2021-10-01 00:00:00"),
        Date.valueOf("2021-10-01"), "BILL");
    Dataset<BillRow> bills = billSource.load(spark, batch.id);

    assertThat("No bills should be read", bills.count(), is(0L));
  }

  @Test
  void canReadBills(SparkSession spark) {
    Batch batch = insertBatchHistoryAndOnBatchCompleted(billingDb, "batch-test", "COMPLETED", Timestamp.valueOf("2021-09-01 00:00:00"),
        Timestamp.valueOf("2021-10-10 00:00:00"), new Timestamp(System.currentTimeMillis()), Timestamp.valueOf("2021-10-01 00:00:00"),
        Date.valueOf("2021-10-01"), "BILL");
    readFromCsvFileAndWriteToExistingTable(billingDb, "input/bill/bill.csv", "bill");

    Dataset<BillRow> bills = billSource.load(spark, batch.id).cache();

    assertThat("Partition number should be the one from config", bills.rdd().getNumPartitions(), is(equalTo(4)));
    assertThat("Read bills should be the same as in the input file", bills.count(), is(1L));
    BillRow bill = bills.collectAsList().get(0);
    assertThat("Bill id should be correct", bill.getBillId(), is("billId1"));
  }
}