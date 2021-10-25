package com.worldpay.pms.bse.engine.transformations.sources;

import static com.worldpay.pms.bse.engine.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.engine.BillingConfiguration;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.PendingMinChargeRow;
import com.worldpay.pms.bse.engine.utils.WithDatabaseAndSpark;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.JdbcSourceConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.utils.DatabaseCsvUtils;
import com.worldpay.pms.testing.utils.DbUtils;
import java.time.LocalDate;
import java.time.LocalDateTime;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class PendingMinChargeSourceTest implements WithDatabaseAndSpark {

  private SqlDb billingDb;
  private SqlDb cisadmDb;
  private SqlDb mduDb;
  private PendingMinChargeSource minChargeSource;

  @Override
  public void bindBillingConfiguration(BillingConfiguration conf) {
    JdbcSourceConfiguration sourceConfiguration = conf.getSources().getPendingMinCharge();
    JdbcSourceConfiguration minCharge = conf.getSources().getMinCharge();
    this.minChargeSource = new PendingMinChargeSource(sourceConfiguration, minCharge, LocalDate.parse("2021-04-27"));
  }

  @Override
  public void bindBillingJdbcConfiguration(JdbcConfiguration conf) {
    this.billingDb = SqlDb.simple(conf);
  }

  @Override
  public void bindMduJdbcConfiguration(JdbcConfiguration conf) {
    mduDb = SqlDb.simple(conf);
  }

  @Override
  public void bindCisadmJdbcConfiguration(JdbcConfiguration conf) {
    this.cisadmDb = SqlDb.simple(conf);
  }

  @Test
  void canFetchStandardBillableItemsWhenNone(SparkSession spark) {
    DbUtils.cleanUp(billingDb, "pending_min_charge");
    cleanupMinChargeData();
    BatchId batchId = createBatchId(LocalDate.of(2021, 2, 1), LocalDate.of(2021, 2, 2));
    Dataset<PendingMinChargeRow> minCharges = minChargeSource.load(spark, batchId);
    assertThat(minCharges.count(), is(equalTo(0L)));
  }

  @Test
  public void canReadPendingMinChargeFrom(SparkSession session) {
    DbUtils.cleanUp(billingDb, "pending_min_charge", "minimum_charge_bill");
    cleanupMinChargeData();

    readFromCsvFileAndWriteToExistingTable(billingDb, "input/min-charge/pending_min_charge.csv",
        "pending_min_charge");
    readFromCsvFileAndWriteToExistingTable(billingDb, "input/min-charge/outputs_registry.csv",
        "outputs_registry");
    insertMinChargeData();

    BatchId batchId = createBatchId(LocalDateTime.of(2021, 3, 16, 0, 0),
        LocalDateTime.of(2021, 3, 16, 0, 1));

    Dataset<PendingMinChargeRow> minCharges = minChargeSource.load(session, batchId).cache();

    assertThat(minCharges.rdd().getNumPartitions(), is(equalTo(6)));
    assertThat(minCharges.count(), is(5L));
  }

  @Test
  public void canReadMinimumChargeWhenMultipleHierarchyFrom(SparkSession session) {
    DbUtils.cleanUp(billingDb, "pending_min_charge", "minimum_charge_bill");
    cleanupMinChargeData();
    cleanupAccountData();

    readFromCsvFileAndWriteToExistingTable(billingDb, "input/min-charge/pending_min_charge.csv",
        "pending_min_charge");
    readFromCsvFileAndWriteToExistingTable(billingDb, "input/min-charge/outputs_registry.csv",
        "outputs_registry");
    insertMinChargeData();
    insertAccountData();

    BatchId batchId = createBatchId(LocalDateTime.of(2021, 3, 16, 0, 0),
        LocalDateTime.of(2021, 3, 16, 0, 1));

    Dataset<PendingMinChargeRow> minCharges = minChargeSource.load(session, batchId).cache();

    assertThat(minCharges.rdd().getNumPartitions(), is(equalTo(6)));
    assertThat(minCharges.count(), is(5L));
  }

  @Test
  public void canReadMinimumChargeWhenWeAlreadyHaveABillFrom(SparkSession session) {
    DbUtils.cleanUp(billingDb, "pending_min_charge", "minimum_charge_bill");
    cleanupMinChargeData();
    cleanupAccountData();

    readFromCsvFileAndWriteToExistingTable(billingDb, "input/min-charge/pending_min_charge.csv",
        "pending_min_charge");
    readFromCsvFileAndWriteToExistingTable(billingDb, "input/min-charge/min_charge_bills.csv",
        "minimum_charge_bill");
    readFromCsvFileAndWriteToExistingTable(billingDb, "input/min-charge/outputs_registry.csv",
        "outputs_registry");
    insertMinChargeData();
    insertAccountData();

    BatchId batchId = createBatchId(LocalDateTime.of(2021, 3, 16, 0, 0),
        LocalDateTime.of(2021, 3, 16, 0, 1));

    Dataset<PendingMinChargeRow> minCharges = minChargeSource.load(session, batchId).cache();

    assertThat(minCharges.rdd().getNumPartitions(), is(equalTo(6)));
    assertThat(minCharges.count(), is(4L));
  }

  private void cleanupMinChargeData(){
    DbUtils.cleanUpWithoutMetadata(cisadmDb, "ci_per");
    DbUtils.cleanUpWithoutMetadata(cisadmDb, "ci_per_char");
    DbUtils.cleanUpWithoutMetadata(cisadmDb, "ci_per_id");
    DbUtils.cleanUpWithoutMetadata(cisadmDb, "ci_priceasgn");
    DbUtils.cleanUpWithoutMetadata(cisadmDb, "ci_pricecomp");
    DbUtils.cleanUpWithoutMetadata(cisadmDb, "ci_party");
  }

  private void insertMinChargeData(){
    DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable(cisadmDb, "input/static-data/ci_per.csv", "ci_per");
    DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable(cisadmDb, "input/static-data/ci_per_char.csv", "ci_per_char");
    DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable(cisadmDb, "input/static-data/ci_per_id.csv", "ci_per_id");
    DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable(cisadmDb, "input/static-data/ci_priceasgn.csv", "ci_priceasgn");
    DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable(cisadmDb, "input/static-data/ci_pricecomp.csv", "ci_pricecomp");
    DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable(cisadmDb, "input/static-data/ci_party.csv", "ci_party");
  }

  private void cleanupAccountData(){
    DbUtils.cleanUpWithoutMetadata(mduDb, "BATCH_HISTORY");
    DbUtils.cleanUpWithoutMetadata(mduDb, "ACCT");
    DbUtils.cleanUpWithoutMetadata(mduDb, "ACCT_HIER");
  }

  private void insertAccountData(){
    readFromCsvFileAndWriteToExistingTable(mduDb, "input/account-data/batch_history.csv",
        "BATCH_HISTORY");
    readFromCsvFileAndWriteToExistingTable(mduDb, "input/account-data/acct_chrg.csv", "ACCT");
    readFromCsvFileAndWriteToExistingTable(mduDb, "input/account-data/acct_hier_chrg.csv", "ACCT_HIER");
  }

}
