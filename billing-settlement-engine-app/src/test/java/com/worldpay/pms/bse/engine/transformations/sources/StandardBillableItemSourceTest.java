package com.worldpay.pms.bse.engine.transformations.sources;

import static com.google.common.collect.Iterators.forArray;
import static com.worldpay.pms.bse.engine.utils.DatabaseCsvUtils.checkThatDataFromCsvFileIsTheSameAsThatInList;
import static com.worldpay.pms.bse.engine.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.domain.model.BillableItem;
import com.worldpay.pms.bse.engine.BillingConfiguration;
import com.worldpay.pms.bse.engine.BillingConfiguration.BillableItemSourceConfiguration;
import com.worldpay.pms.bse.engine.BillingConfiguration.FailedBillableItemSourceConfiguration;
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
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StandardBillableItemSourceTest implements WithDatabaseAndSpark {

  private SqlDb pricingDb;
  private SqlDb billingDb;
  private StandardBillableItemSource source;
  private BillableItemSourceConfiguration sourceConfiguration;
  private FailedBillableItemSourceConfiguration failedSourceConfiguration;
  private int maxAttempts;

  @Override
  public void bindBillingConfiguration(BillingConfiguration conf) {
    this.sourceConfiguration = conf.getSources().getBillableItems();
    this.failedSourceConfiguration = conf.getSources().getFailedBillableItems();
    this.maxAttempts = conf.getMaxAttempts();
    this.source = new StandardBillableItemSource(this.sourceConfiguration, this.failedSourceConfiguration, Option.none(), maxAttempts);
  }

  @Override
  public void bindBillingJdbcConfiguration(JdbcConfiguration conf) {
    this.billingDb = SqlDb.simple(conf);
  }

  @Override
  public void bindPricingJdbcConfiguration(JdbcConfiguration conf) {
    this.pricingDb = SqlDb.simple(conf);
  }

  @AfterAll
  static void afterAll() {
    SparkContext.reset();
  }

  @BeforeEach
  void cleanup() {
    DbUtils.cleanUpWithoutMetadata(pricingDb, "billable_charge", "billable_charge_line", "billable_charge_service_output");
    DbUtils.cleanUp(billingDb, "bill_item_error");
  }

  @Test
  void canFetchStandardBillableItemsWhenNone(SparkSession spark) {
    BatchId batchId = createBatchId(LocalDate.parse("2019-12-01"), LocalDate.parse("2019-12-02"));

    Dataset<BillableItemRow> txns = source.load(spark, batchId);
    assertThat(txns.count(), is(equalTo(0L)));
  }

  @Test
  public void canReadStandardBillableItemsFrom(SparkSession session) {

    readFromCsvFileAndWriteToExistingTable(pricingDb, "input/standard-billable-item/billable_charge.csv",
        "billable_charge");
    readFromCsvFileAndWriteToExistingTable(pricingDb, "input/standard-billable-item/billable_charge_line.csv",
        "billable_charge_line");
    readFromCsvFileAndWriteToExistingTable(pricingDb, "input/standard-billable-item/billable_charge_service_output.csv",
        "billable_charge_service_output");

    BatchId batchId = createBatchId(LocalDateTime.parse("2020-09-16T00:00:00"), LocalDateTime.parse("2020-09-16T00:01:00"));

    Dataset<BillableItemRow> billableItems = source.load(session, batchId).cache();
    //after shuffle
    assertThat(billableItems.rdd().getNumPartitions(), is(equalTo(4)));
    assertThat(billableItems.count(), is(2L));

    checkThatDataFromCsvFileIsTheSameAsThatInList("input/standard-billable-item/billable_item_row_expected.csv",
        billableItems.collectAsList(), BillableItemRow.class);

    checkThatDataFromCsvFileIsTheSameAsThatInList("input/standard-billable-item/billable_item_line_row_expected.csv",
        billableItems
            .flatMap(i -> forArray(i.getBillableItemLines()), Encodings.BILLABLE_ITEM_LINE_ROW_ENCODER)
            .collectAsList(), BillableItemLineRow.class);
  }

  @Test
  public void canOverrideRateType(SparkSession session) {
    readFromCsvFileAndWriteToExistingTable(pricingDb, "input/standard-billable-item/billable_charge.csv",
        "billable_charge");
    readFromCsvFileAndWriteToExistingTable(pricingDb, "input/standard-billable-item/billable_charge_line.csv",
        "billable_charge_line");
    readFromCsvFileAndWriteToExistingTable(pricingDb, "input/standard-billable-item/billable_charge_service_output.csv",
        "billable_charge_service_output");

    BatchId batchId = createBatchId(LocalDateTime.parse("2020-09-17T00:00:00"), LocalDateTime.parse("2020-09-17T00:01:00"));

    List<BillableItemRow> billableItems = source.load(session, batchId).collectAsList();

    assertThat(billableItems.size(), is(1));
    assertThat(billableItems.get(0).getBillableItemLines().length, is(5));
    Arrays.stream(billableItems.get(0).getBillableItemLines()).forEach(line -> assertThat(line.getRateType(), is("NA")));
  }

  @Test
  public void canReadFailedBillableItems(SparkSession session) {

    SourceTestUtil.insertBatchHistoryAndOnBatchCompleted(billingDb, "std_bill_item_test", "COMPLETED",
        Timestamp.valueOf("2020-12-30 00:00:00"), Timestamp.valueOf("2021-12-31 00:00:00"), new Timestamp(System.currentTimeMillis()),
        Timestamp.valueOf("2020-12-31 00:00:00"), Date.valueOf("2020-11-30"), "BILL_ITEM_ERROR");

    readFromCsvFileAndWriteToExistingTable(pricingDb, "input/standard-billable-item/billable_charge.csv",
        "billable_charge");
    readFromCsvFileAndWriteToExistingTable(pricingDb, "input/standard-billable-item/billable_charge_line.csv",
        "billable_charge_line");
    readFromCsvFileAndWriteToExistingTable(pricingDb, "input/standard-billable-item/billable_charge_service_output.csv",
        "billable_charge_service_output");
    readFromCsvFileAndWriteToExistingTable(billingDb, "input/standard-billable-item/bill_item_error.csv", "bill_item_error");

    BatchId batchId = createBatchId(LocalDateTime.parse("2020-12-30T00:00:00"), LocalDateTime.parse("2020-12-31T00:01:00"));

    Dataset<BillableItemRow> billableItems = source.load(session, batchId).cache();
    List<BillableItemRow> billableItemsList = billableItems.collectAsList();

    assertThat(billableItemsList.size(), is(1));
    assertThat(billableItemsList.get(0).getBillableItemLines().length, is(4));
    assertThat(billableItemsList.get(0).getBillableItemServiceQuantity().getServiceQuantities().size(), is(6));
  }

  @Test
  public void canOverrideAccruedDate(SparkSession session) {
    StandardBillableItemSource source = new StandardBillableItemSource(this.sourceConfiguration, this.failedSourceConfiguration,
        Option.of(Date.valueOf(LocalDate.now())), maxAttempts);

    SourceTestUtil.insertBatchHistoryAndOnBatchCompleted(billingDb, "std_bill_item_test", "COMPLETED",
        Timestamp.valueOf("2020-12-30 00:00:00"), Timestamp.valueOf("2021-12-31 00:00:00"), new Timestamp(System.currentTimeMillis()),
        Timestamp.valueOf("2020-12-31 00:00:00"), Date.valueOf("2020-11-30"), "BILL_ITEM_ERROR");

    readFromCsvFileAndWriteToExistingTable(pricingDb, "input/standard-billable-item/billable_charge.csv",
        "billable_charge");
    readFromCsvFileAndWriteToExistingTable(pricingDb, "input/standard-billable-item/billable_charge_line.csv",
        "billable_charge_line");
    readFromCsvFileAndWriteToExistingTable(pricingDb, "input/standard-billable-item/billable_charge_service_output.csv",
        "billable_charge_service_output");
    readFromCsvFileAndWriteToExistingTable(billingDb, "input/standard-billable-item/bill_item_error.csv", "bill_item_error");

    BatchId batchId = createBatchId(LocalDateTime.parse("2020-12-30T00:00:00"), LocalDateTime.parse("2020-12-31T00:01:00"));

    List<BillableItemRow> billableItems = source.load(session, batchId).collectAsList();

    billableItems.forEach(item -> assertThat(item.getAccruedDate(), is(Date.valueOf(LocalDate.now()))));
  }

}
