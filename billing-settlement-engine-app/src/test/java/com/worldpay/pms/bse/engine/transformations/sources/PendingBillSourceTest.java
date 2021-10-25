package com.worldpay.pms.bse.engine.transformations.sources;

import static com.worldpay.pms.bse.engine.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static com.worldpay.pms.bse.engine.utils.SourceTestUtil.insertBatchHistoryAndOnBatchCompleted;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.worldpay.pms.bse.engine.BillingConfiguration;
import com.worldpay.pms.bse.engine.BillingConfiguration.PendingConfiguration;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import com.worldpay.pms.bse.engine.utils.WithDatabaseAndSpark;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.junit.SparkContext;
import com.worldpay.pms.testing.utils.DbUtils;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
public class PendingBillSourceTest implements WithDatabaseAndSpark {

  private static final String ROOT_DIR = "input/pending-bill/";
  private static final String FIELD_SHOULD_NOT_BE_NULL = "Field should not be null.";

  private SqlDb billingDb;
  private PendingBillSource pendingBillSource;

  @Override
  public void bindBillingConfiguration(BillingConfiguration conf) {
    PendingConfiguration pendingConfiguration = conf.getSources().getPendingConfiguration();

    this.pendingBillSource = new PendingBillSource(pendingConfiguration);
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
  public void cleanUp() {
    DbUtils.cleanUp(billingDb, "pending_bill_line_svc_qty", "pending_bill_line_calc", "pending_bill_line", "pending_bill");
  }

  @Test
  void testEmptyInput(SparkSession spark) {
    Batch batch = insertBatchHistoryAndOnBatchCompleted(billingDb, "empty_test", "COMPLETED", Timestamp.valueOf("2020-09-25 00:00:00"),
        Timestamp.valueOf("2020-10-05 00:00:00"), new Timestamp(System.currentTimeMillis()), Timestamp.valueOf("2020-10-01 00:00:00"),
        Date.valueOf("2020-10-01"), "PENDING_BILL");
    Dataset<PendingBillRow> txns = pendingBillSource.load(spark, batch.id);

    assertThat("No pending bills should be created", txns.count(), is(0L));
  }

  @Test
  void whenMultipleDateIntervalsArePresentThenTheDatasetWillContainOnlyTheEventsFromTheLastRun(SparkSession session) {

    readFromCsvFilesAndWriteToExistingTables();

    Timestamp low = Timestamp.valueOf("2020-09-20 00:00:00");
    Timestamp high = Timestamp.valueOf("2020-09-25 00:00:00");
    Batch batch = insertBatchHistoryAndOnBatchCompleted(billingDb, "test", "COMPLETED", low, high,
        new Timestamp(System.currentTimeMillis()), Timestamp.valueOf("2020-10-01 00:00:00"), Date.valueOf("2020-10-01"), "PENDING_BILL");

    Dataset<PendingBillRow> pendingBills = pendingBillSource.load(session, batch.id).cache();
    List<PendingBillRow> pendingBillList = pendingBills.collectAsList();
    pendingBillList.forEach(PendingBillSourceTest::assertConstraintsOnFieldsForPendingBill);
    assertThat("No of pending bills should be equal to the lines in pending_bill from the last run", pendingBillList.size(), is(2));
    assertThat("No of pending bill lines should be equal to the aggregated (by bill_id) no of lines in pending_bill_line from the last run",
        pendingBillList.get(0).getPendingBillLines().length, is(1));
  }

  @Test
  void whenDataIsLoadedFromInputTablesThenDatasetIsGeneratedWithTheCorrectStructureAndValues(SparkSession session) {

    readFromCsvFilesAndWriteToExistingTables();

    Timestamp low = Timestamp.valueOf("2020-09-25 00:00:00");
    Timestamp high = Timestamp.valueOf("2020-10-05 00:00:00");
    Batch batch = insertBatchHistoryAndOnBatchCompleted(billingDb, "test", "COMPLETED", low, high,
        new Timestamp(System.currentTimeMillis()), Timestamp.valueOf("2020-10-01 00:00:00"), Date.valueOf("2020-10-01"), "PENDING_BILL");

    Dataset<PendingBillRow> pendingBills = pendingBillSource.load(session, batch.id).cache();
    List<PendingBillRow> pendingBillList = pendingBills.collectAsList();
    pendingBillList.forEach(PendingBillSourceTest::assertConstraintsOnFieldsForPendingBill);
    assertThat("No of pending bills should be equal to the lines in pending_bill from the last run", pendingBillList.size(),
        is(2));
    PendingBillRow pendingBill1 = null;
    PendingBillRow pendingBill2 = null;

    for (PendingBillRow pB : pendingBillList) {
      if ("bill_id_1".equals(pB.getBillId())) {
        pendingBill1 = pB;
      }
      if ("bill_id_2".equals(pB.getBillId())) {
        pendingBill2 = pB;
      }
    }

    assertThat("Pending bill should exist", pendingBill1, is(notNullValue()));
    assertThat("Pending bill should exist", pendingBill2, is(notNullValue()));
    assertThat(
        "No of pending bill lines should be equal to the aggregated (by bill_id) no of lines in pending_bill_line from the last run",
        pendingBill1.getPendingBillLines().length, is(3));
    assertThat(
        "No of pending bill lines should be equal to the aggregated (by bill_id) no of lines in pending_bill_line from the last run",
        pendingBill2.getPendingBillLines().length, is(1));
    PendingBillLineRow pendingBillLine4 = pendingBill2.getPendingBillLines()[0];
    assertThat("Pending bill line should have correct value", pendingBillLine4.getBillLineId(), is("bill_ln_id_4"));
    assertThat("Pending bill line should have correct value", pendingBillLine4.getBillLinePartyId(), is("PO4007445930"));

    assertThat(
        "No of pending bill line calcs should be equal to the aggregated (by bill_line_id) no of lines in pending_bill_line_calc from the last run",
        Arrays.stream(pendingBill1.getPendingBillLines()).map(PendingBillLineRow::getPendingBillLineCalculations).reduce(ArrayUtils::addAll)
            .orElse(new PendingBillLineCalculationRow[0]).length, is(6));
    assertThat(
        "No of pending bill line calcs should be equal to the aggregated (by bill_line_id) no of lines in pending_bill_line_calc from the last run",
        Arrays.stream(pendingBill2.getPendingBillLines()).map(PendingBillLineRow::getPendingBillLineCalculations).reduce(ArrayUtils::addAll)
            .orElse(new PendingBillLineCalculationRow[0]).length, is(1));

    assertThat(
        "No of pending bill line svc qty should be equal to the aggregated (by bill_line_id) no of lines in pending_bill_line_svc_qty from the last run",
        Arrays.stream(pendingBill1.getPendingBillLines()).map(PendingBillLineRow::getPendingBillLineServiceQuantities)
            .reduce(ArrayUtils::addAll)
            .orElse(new PendingBillLineServiceQuantityRow[0]).length, is(18));
    assertThat(
        "No of pending bill line svc qty should be equal to the aggregated (by bill_line_id) no of lines in pending_bill_line_svc_qty from the last run",
        Arrays.stream(pendingBill2.getPendingBillLines()).map(PendingBillLineRow::getPendingBillLineServiceQuantities)
            .reduce(ArrayUtils::addAll)
            .orElse(new PendingBillLineServiceQuantityRow[0]).length, is(6));
  }

  private void readFromCsvFilesAndWriteToExistingTables() {
    readFromCsvFileAndWriteToExistingTable(billingDb, ROOT_DIR + "pending-bill.csv", "pending_bill");
    readFromCsvFileAndWriteToExistingTable(billingDb, ROOT_DIR + "pending-bill-line.csv", "pending_bill_line");
    readFromCsvFileAndWriteToExistingTable(billingDb, ROOT_DIR + "pending-bill-line-calc.csv", "pending_bill_line_calc");
    readFromCsvFileAndWriteToExistingTable(billingDb, ROOT_DIR + "pending-bill-line-svc-qty.csv", "pending_bill_line_svc_qty");
  }

  public static void assertConstraintsOnFieldsForPendingBill(PendingBillRow pendingBill) {
    assertConstraintsOnFields(pendingBill);
    asList(pendingBill.getPendingBillLines()).forEach(pendingBillLineRow -> {
      assertConstraintsOnFields(pendingBillLineRow);
      asList(pendingBillLineRow.getPendingBillLineCalculations()).forEach(PendingBillSourceTest::assertConstraintsOnFields);
      asList(pendingBillLineRow.getPendingBillLineServiceQuantities()).forEach(PendingBillSourceTest::assertConstraintsOnFields);
    });
  }

  private static void assertConstraintsOnFields(PendingBillRow pendingBill) {
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getBillId());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getPartyId());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getLegalCounterpartyId());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getAccountId());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getBillSubAccountId());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getAccountType());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getBusinessUnit());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getBillCycleId());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getScheduleStart());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getScheduleEnd());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getCurrency());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getBillReference());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getAdhocBillIndicator());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getOverpaymentIndicator());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getReleaseWAFIndicator());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getReleaseReserveIndicator());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getFastestPaymentRouteIndicator());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getIndividualBillIndicator());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBill.getManualBillNarrative());
  }

  private static void assertConstraintsOnFields(PendingBillLineRow pendingBillLine) {
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBillLine.getBillLineId());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBillLine.getBillLinePartyId());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBillLine.getProductIdentifier());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBillLine.getQuantity());
  }

  private static void assertConstraintsOnFields(PendingBillLineCalculationRow pendingBillLineCalculation) {
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBillLineCalculation.getBillLineCalcId());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBillLineCalculation.getCalculationLineClassification());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBillLineCalculation.getAmount());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBillLineCalculation.getIncludeOnBill());
  }

  private static void assertConstraintsOnFields(PendingBillLineServiceQuantityRow pendingBillLineServiceQuantity) {
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBillLineServiceQuantity.getServiceQuantityTypeCode());
    assertNotNull(FIELD_SHOULD_NOT_BE_NULL, pendingBillLineServiceQuantity.getServiceQuantityValue());
  }
}