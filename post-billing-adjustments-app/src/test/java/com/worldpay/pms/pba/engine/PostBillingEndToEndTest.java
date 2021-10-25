package com.worldpay.pms.pba.engine;

import static com.worldpay.pms.pba.engine.utils.TableDiffer.combine;
import static com.worldpay.pms.testing.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.worldpay.pms.pba.engine.utils.TableDiffer;
import com.worldpay.pms.pba.engine.utils.WithDatabase;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.junit.SparkContext;
import com.worldpay.pms.testing.stereotypes.WithInMemoryDb;
import com.worldpay.pms.testing.stereotypes.WithSpark;
import com.worldpay.pms.testing.utils.DbUtils;
import com.worldpay.pms.utils.Strings;
import io.vavr.Function2;
import io.vavr.collection.Array;
import io.vavr.collection.CharSeq;
import io.vavr.collection.HashSet;
import io.vavr.control.Option;
import java.sql.Date;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.sql2o.Sql2oQuery;
import org.sql2o.data.Table;

@WithInMemoryDb
@WithSpark
class PostBillingEndToEndTest implements WithDatabase {

  private static final String DATE_2021_02_17 = "2021-02-17";

  private SqlDb billingDb;
  private JdbcConfiguration inputDbConfig;
  private SqlDb inputDb;

  @BeforeEach
  void setUp(SqlDb h2Db, JdbcConfiguration h2DbConfig) {
    this.inputDb = h2Db;
    this.inputDbConfig = h2DbConfig;

    this.inputDb.execQuery("mode", "SET MODE Oracle", Sql2oQuery::executeUpdate);
    Db.TABLES.forEach(view -> Db.createInputTable(billingDb, inputDb, view));
  }

  @Override
  public void bindPostBillingJdbcConfiguration(JdbcConfiguration conf) {
    this.billingDb = SqlDb.simple(conf);
  }

  @BeforeEach
  void cleanup() {
    DbUtils.cleanUpWithoutMetadata(billingDb, "bill", "outputs_registry", "post_bill_adj", "post_bill_adj_accounting",
        "batch_history_post_billing", "cm_bill_payment_dtl", "cm_bill_payment_dtl_snapshot");

    readFromCsvFileAndWriteToExistingTable(inputDb, "end-to-end/withhold-funds.csv", "vw_withhold_funds");
    readFromCsvFileAndWriteToExistingTable(inputDb, "end-to-end/sub_acct.csv", "vw_sub_acct");
    readFromCsvFileAndWriteToExistingTable(inputDb, "end-to-end/vwm_merch_acct_ledger_snapshot.csv", "vwm_merch_acct_ledger_snapshot");

    readFromCsvFileAndWriteToExistingTable(billingDb, "end-to-end/outputs_registry.csv", "outputs_registry");
  }

  @Test
  @DisplayName("When there is no withhold fund setup for account then create no adjustment")
  void testNoWaf() {
    insertDataAndRun("no-waf");

    Option<String> postBillAdjComparisonResult = comparePostBillAdj();
    assertThat(postBillAdjComparisonResult, is(Option.none()));
  }

  @Test
  @DisplayName("When merchant has withhold fund with null target and 100 percent then hold all funds")
  void whenMerchantHasWafAndTargetIsNullAddAdjustment() {
    insertDataAndRun("waf-null-target");

    Option<String> postBillAdjComparisonResult = comparePostBillAdj();
    assertThat(postBillAdjComparisonResult, is(Option.none()));

    Option<String> postBillAdjAccountingComparisonResult = comparePostBillAdjAccounting();
    assertThat(postBillAdjAccountingComparisonResult, is(Option.none()));

    assertBillPaymentIsPublished(2,2);
  }

  @Test
  @DisplayName("When merchant has withhold funds with null target and percent ! = 100 then hold just the right percent of the bills")
  void whenMerchantHasWafAndTargetIsNullAndPercentAddAdjustment() {
    insertDataAndRun("waf-null-target-percent");

    Option<String> postBillAdjComparisonResult = comparePostBillAdj();
    assertThat(postBillAdjComparisonResult, is(Option.none()));

    Option<String> postBillAdjAccountingComparisonResult = comparePostBillAdjAccounting();
    assertThat(postBillAdjAccountingComparisonResult, is(Option.none()));

    assertBillPaymentIsPublished(2,2);
  }

  @Test
  @DisplayName("When merchant has withhold funds with target and percent set then hold just the right target")
  void whenMerchantHasWafAndTargetIsNotNullAndPercentAddAdjustment() {
    insertDataAndRun("waf-target-percent");

    Option<String> postBillAdjComparisonResult = comparePostBillAdj();
    assertThat(postBillAdjComparisonResult, is(Option.none()));

    Option<String> postBillAdjAccountingComparisonResult = comparePostBillAdjAccounting();
    assertThat(postBillAdjAccountingComparisonResult, is(Option.none()));

    assertBillPaymentIsPublished(2,2);
  }

  @Test
  @DisplayName("When merchant has withhold funds set with target and we can find a current waf balance then hold just the right amount")
  void whenMerchantHasWafAndTargetIsNotNullAndPercentAndCurrentBalanceAddAdjustment() {
    insertDataAndRun("waf-target-current-balance");

    Option<String> postBillAdjComparisonResult = comparePostBillAdj();
    assertThat(postBillAdjComparisonResult, is(Option.none()));

    Option<String> postBillAdjAccountingComparisonResult = comparePostBillAdjAccounting();
    assertThat(postBillAdjAccountingComparisonResult, is(Option.none()));

    assertBillPaymentIsPublished(1,1);
  }

  private void insertDataAndRun(String directoryName) {
    readFromCsvFileAndWriteToExistingTable(billingDb, String.format("end-to-end/%s/bill.csv", directoryName), "bill");
    readFromCsvFileAndWriteToExistingTable(inputDb, String.format("end-to-end/%s/post_bill_adjustment.csv", directoryName),
        "vw_post_bill_adj");
    readFromCsvFileAndWriteToExistingTable(inputDb, String.format("end-to-end/%s/post_bill_adjustment_accounting.csv", directoryName),
        "vw_post_bill_adj_accounting");

    run(DATE_2021_02_17);
  }

  private void run(String logicalDate) {
    SparkContext.cleanUp();
    TestPostBillingEngine.submit(Date.valueOf(logicalDate), inputDbConfig);
  }

  private Option<String> comparePostBillAdj() {

    return compare(
        "vw_post_bill_adj",
        "vw_post_bill_adj",
        TableDiffer::schemaMatches,
        TableDiffer::rowCountMatch,
        this::postBillAdjDataComparison
    );
  }

  Option<String> postBillAdjDataComparison(Table actual, Table expected) {
    return TableDiffer.dataMatches(actual, expected, Array.of("bill_id"),
        HashSet.of("batch_code", "batch_attempt", "ilm_dt", "cre_dttm", "post_bill_adj_id", "source_key", "non_event_id"));
  }

  private Option<String> comparePostBillAdjAccounting() {

    return compare(
        "vw_post_bill_adj_accounting",
        "vw_post_bill_adj_accounting",
        TableDiffer::schemaMatches,
        TableDiffer::rowCountMatch,
        this::postBillAdjAccountingDataComparison
    );
  }

  Option<String> postBillAdjAccountingDataComparison(Table actual, Table expected) {
    return TableDiffer.dataMatches(actual, expected, Array.of("bill_id", "sub_acct_id"),
        HashSet.of("batch_code", "batch_attempt", "ilm_dt", "cre_dttm", "post_bill_adj_id", "partition_id"));
  }

  /*
  * compare actual table in billing database with the expected table in H2 db
   */
  Option<String> compare(String actual, String expected, Function2<Table, Table, Option<String>>... checks) {
    String header = format("Differences found when comparing `%s` and `%s`:", actual.toUpperCase(), expected.toUpperCase());
    Table actualTable = billingDb.execQuery(actual, format("SELECT * FROM %s", actual), Sql2oQuery::executeAndFetchTable);
    Table expectedTable = inputDb.execQuery(expected, format("SELECT * FROM %s", expected), Sql2oQuery::executeAndFetchTable);

    return combine(Array.of(checks).map(check -> check.apply(actualTable, expectedTable)))
        .filter(Strings::isNotNullOrEmptyOrWhitespace)
        .map(error -> format("%s\n%s\n%s\n", header, CharSeq.repeat('*', header.length() + 1), error));
  }

  private void assertBillPaymentIsPublished(long billPaymentCount, long billPaymentSnapshotCount){
    assertEquals(billPaymentCount, DbUtils.getCount(billingDb, "cm_bill_payment_dtl"));
    assertEquals(billPaymentSnapshotCount, DbUtils.getCount(billingDb, "cm_bill_payment_dtl_snapshot"));
  }
}
