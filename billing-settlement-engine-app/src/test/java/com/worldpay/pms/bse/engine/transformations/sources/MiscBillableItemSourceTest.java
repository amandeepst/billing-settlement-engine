package com.worldpay.pms.bse.engine.transformations.sources;

import static com.google.common.collect.Iterators.forArray;
import static com.worldpay.pms.bse.engine.utils.DatabaseCsvUtils.checkThatDataFromCsvFileIsTheSameAsThatInList;
import static com.worldpay.pms.bse.engine.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.engine.BillingConfiguration;
import com.worldpay.pms.bse.engine.BillingConfiguration.FailedBillableItemSourceConfiguration;
import com.worldpay.pms.bse.engine.BillingConfiguration.MiscBillableItemSourceConfiguration;
import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemLineRow;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemRow;
import com.worldpay.pms.bse.engine.utils.SourceTestUtil;
import com.worldpay.pms.bse.engine.utils.WithDatabaseAndSpark;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.junit.SparkContext;
import com.worldpay.pms.testing.utils.DbUtils;
import io.vavr.control.Option;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MiscBillableItemSourceTest implements WithDatabaseAndSpark {

  private SqlDb chargeUploadDb;
  private SqlDb billingDb;
  private MiscBillableItemSource source;
  private MiscBillableItemSourceConfiguration sourceConfiguration;
  private FailedBillableItemSourceConfiguration failedSourceConfiguration;
  private int maxAttempts;

  @Override
  public void bindChargeUploadJdbcConfiguration(JdbcConfiguration conf) {
    this.chargeUploadDb = SqlDb.simple(conf);
  }

  @Override
  public void bindBillingConfiguration(BillingConfiguration conf) {
    this.sourceConfiguration = conf.getSources().getMiscBillableItems();
    this.failedSourceConfiguration = conf.getSources().getFailedBillableItems();
    this.maxAttempts = conf.getMaxAttempts();
    this.source = new MiscBillableItemSource(sourceConfiguration, failedSourceConfiguration, Option.none(), maxAttempts);
  }

  @Override
  public void bindBillingJdbcConfiguration(JdbcConfiguration conf) {
    this.billingDb = SqlDb.simple(conf);
  }

  @AfterAll
  static void afterAll() {
    SparkContext.reset();
  }

  @BeforeEach
  void cleanup() {
    DbUtils.cleanUpWithoutMetadata(chargeUploadDb, "cm_misc_bill_item", "cm_misc_bill_item_ln", "batch_history");
    DbUtils.cleanUp(billingDb, "bill_item_error");
  }

  @Test
  void canReadMiscBillableItemsWhenNone(SparkSession spark) {
    BatchId batchId = createBatchId(LocalDate.parse("2020-11-01", ISO_DATE), LocalDate.parse("2020-11-02", ISO_DATE));
    Dataset<BillableItemRow> billableItems = source.load(spark, batchId);
    assertThat(billableItems.count(), is(equalTo(0L)));
  }

  @Test
  void canReadMiscBillableItemsWithStatusDifferentThanInactive(SparkSession session) {
    readFromCsvFileAndWriteToExistingTable(chargeUploadDb, "input/misc-billable-item/batch-history.csv", "batch_history");
    readFromCsvFileAndWriteToExistingTable(chargeUploadDb, "input/misc-billable-item/misc-billable-item.csv", "cm_misc_bill_item");
    readFromCsvFileAndWriteToExistingTable(chargeUploadDb, "input/misc-billable-item/misc-billable-item-line.csv", "cm_misc_bill_item_ln");

    BatchId batchId = createBatchId(LocalDate.parse("2020-11-09"), LocalDate.parse("2020-11-10"));

    Dataset<BillableItemRow> billableItems = source.load(session, batchId).cache();

    assertThat(billableItems.count(), is(2L));

    checkThatDataFromCsvFileIsTheSameAsThatInList("input/misc-billable-item/expected-misc-billable-item-row.csv",
        billableItems.collectAsList(), BillableItemRow.class);
    checkThatDataFromCsvFileIsTheSameAsThatInList("input/misc-billable-item/expected-misc-billable-item-line-row.csv",
        billableItems
            .flatMap(i -> forArray(i.getBillableItemLines()), Encodings.BILLABLE_ITEM_LINE_ROW_ENCODER)
            .collectAsList(), BillableItemLineRow.class);
    assertThat(billableItems.collectAsList().get(0).getPriceCurrency(), is("GBP"));
  }

  @Test
  void canReadFailedMiscBillableItems(SparkSession session) {
    SourceTestUtil.insertBatchHistoryAndOnBatchCompleted(billingDb, "misc_bill_item_test", "COMPLETED",
        Timestamp.valueOf("2020-11-30 00:00:00"), Timestamp.valueOf("2021-12-01 00:00:00"), new Timestamp(System.currentTimeMillis()),
        Timestamp.valueOf("2020-12-01 00:00:00"), Date.valueOf("2020-12-01"), "BILL_ITEM_ERROR");

    readFromCsvFileAndWriteToExistingTable(chargeUploadDb, "input/misc-billable-item/batch-history.csv", "batch_history");
    readFromCsvFileAndWriteToExistingTable(chargeUploadDb, "input/misc-billable-item/misc-billable-item.csv", "cm_misc_bill_item");
    readFromCsvFileAndWriteToExistingTable(chargeUploadDb, "input/misc-billable-item/misc-billable-item-line.csv", "cm_misc_bill_item_ln");
    readFromCsvFileAndWriteToExistingTable(billingDb, "input/standard-billable-item/bill_item_error.csv", "bill_item_error");

    BatchId batchId = createBatchId(LocalDateTime.parse("2020-11-30T00:00:00"), LocalDateTime.parse("2020-12-01T06:00:00"));

    List<BillableItemRow> failedBillableItems = source.load(session, batchId).collectAsList();

    assertThat(failedBillableItems.size(), is(1));
    assertThat(failedBillableItems.get(0).getBillableItemLines().length, is(1));
  }

  @Test
  void canOverrideAccruedDate(SparkSession session) {
    MiscBillableItemSource source = new MiscBillableItemSource(sourceConfiguration, failedSourceConfiguration,
        Option.of(Date.valueOf(LocalDate.now())), maxAttempts);

    SourceTestUtil.insertBatchHistoryAndOnBatchCompleted(billingDb, "misc_bill_item_test", "COMPLETED",
        Timestamp.valueOf("2020-11-30 00:00:00"), Timestamp.valueOf("2021-12-01 00:00:00"), new Timestamp(System.currentTimeMillis()),
        Timestamp.valueOf("2020-12-01 00:00:00"), Date.valueOf("2020-12-01"), "BILL_ITEM_ERROR");

    readFromCsvFileAndWriteToExistingTable(chargeUploadDb, "input/misc-billable-item/batch-history.csv", "batch_history");
    readFromCsvFileAndWriteToExistingTable(chargeUploadDb, "input/misc-billable-item/misc-billable-item.csv", "cm_misc_bill_item");
    readFromCsvFileAndWriteToExistingTable(chargeUploadDb, "input/misc-billable-item/misc-billable-item-line.csv", "cm_misc_bill_item_ln");
    readFromCsvFileAndWriteToExistingTable(billingDb, "input/standard-billable-item/bill_item_error.csv", "bill_item_error");

    BatchId batchId = createBatchId(LocalDateTime.parse("2020-11-30T00:00:00"), LocalDateTime.parse("2020-12-01T06:00:00"));

    List<BillableItemRow> failedBillableItems = source.load(session, batchId).collectAsList();

    failedBillableItems.forEach(item -> assertThat(item.getAccruedDate(), is(Date.valueOf(LocalDate.now()))));
  }
}
