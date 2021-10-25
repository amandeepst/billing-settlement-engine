package com.worldpay.pms.bse.engine.transformations.sources;

import static com.worldpay.pms.bse.engine.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static com.worldpay.pms.bse.engine.utils.SourceTestUtil.insertBatchHistoryAndOnBatchCompleted;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTaxDetail;
import com.worldpay.pms.bse.engine.BillingConfiguration;
import com.worldpay.pms.bse.engine.utils.WithDatabaseAndSpark;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.JdbcSourceConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.junit.SparkContext;
import com.worldpay.pms.testing.utils.DbUtils;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BillTaxSourceTest implements WithDatabaseAndSpark {

  private SqlDb billingDb;
  private SqlDb cisadmDb;
  private BillTaxSource billTaxSource;
  private JdbcSourceConfiguration billTaxConfiguration;

  @Override
  public void bindBillingConfiguration(BillingConfiguration conf) {
    billTaxConfiguration = conf.getSources().getBillTax();
  }

  @Override
  public void bindBillingJdbcConfiguration(JdbcConfiguration conf) {
    this.billingDb = SqlDb.simple(conf);
  }

  @Override
  public void bindCisadmJdbcConfiguration(JdbcConfiguration conf) {
    this.cisadmDb = SqlDb.simple(conf);
  }

  @AfterAll
  static void afterAll() {
    SparkContext.reset();
  }

  @BeforeEach
  public void cleanUp() {
    DbUtils.cleanUpWithoutMetadata(cisadmDb, "cm_inv_recalc_stg");
    DbUtils.cleanUp(billingDb, "bill_tax", "bill_tax_detail");
  }

  @AfterEach
  void afterEach() {
    DbUtils.cleanUpWithoutMetadata(cisadmDb, "cm_inv_recalc_stg");
  }

  @Test
  void testEmptyInput(SparkSession spark) {

    billTaxSource = new BillTaxSource(billTaxConfiguration);

    Batch batch = insertBatchHistoryAndOnBatchCompleted(billingDb, "empty_test", "COMPLETED", Timestamp.valueOf("2020-09-25 00:00:00"),
        Timestamp.valueOf("2020-10-05 00:00:00"), new Timestamp(System.currentTimeMillis()), Timestamp.valueOf("2020-10-01 00:00:00"),
        Date.valueOf("2020-10-01"), "BILL");
    Dataset<BillTax> billTaxes = billTaxSource.load(spark, batch.id);

    assertThat("No bill taxes should be created", billTaxes.count(), is(0L));
  }

  @Test
  void whenDataIsPresentInDbThenReaderWillLoadItInDatasource(SparkSession spark) {
    readFromCsvFilesAndWriteToExistingTables();
    billTaxSource = new BillTaxSource(billTaxConfiguration);

    Batch batch = insertBatchHistoryAndOnBatchCompleted(billingDb, "test", "COMPLETED", Timestamp.valueOf("2020-09-25 00:00:00"),
        Timestamp.valueOf("2020-10-05 00:00:00"), new Timestamp(System.currentTimeMillis()), Timestamp.valueOf("2020-10-01 00:00:00"),
        Date.valueOf("2020-10-01"), "BILL");
    List<BillTax> billTaxes = billTaxSource.load(spark, batch.id).collectAsList();

    assertThat("Bill tax should be created", billTaxes.size(), is(1));
    BillTax billTax = billTaxes.get(0);
    assertThat("Bill tax id should be correct", billTax.getBillTaxId(), is("billTaxId1"));
    assertThat("Bill id should be correct", billTax.getBillId(), is("bill_id_1"));
    assertThat("Worldpay registration number should be correct", billTax.getWorldpayTaxRegistrationNumber(), is("wp_reg_1"));
    BillTaxDetail[] billTaxDetails = billTax.getBillTaxDetails();
    assertNotNull("Bill tax details should be created", billTaxDetails);
    Arrays.stream(billTaxDetails).forEach(billTaxDetail -> {
      assertThat("Bill tax detail: tax status, should be correct", billTaxDetail.getTaxStatus(), in(Arrays.asList("EXE", "S")));
      assertThat("Bill tax detail: tax rate, should be correct", billTaxDetail.getTaxRate().setScale(0),
          in(Arrays.asList(BigDecimal.ZERO, BigDecimal.valueOf(21))));
    });
  }

  private void readFromCsvFilesAndWriteToExistingTables() {
    readFromCsvFileAndWriteToExistingTable(cisadmDb, "input/bill-tax/cm_inv_recalc_stg.csv", "cm_inv_recalc_stg");
    readFromCsvFileAndWriteToExistingTable(billingDb, "input/bill-tax/bill-tax.csv", "bill_tax");
    readFromCsvFileAndWriteToExistingTable(billingDb, "input/bill-tax/bill-tax-detail.csv", "bill_tax_detail");
  }

}