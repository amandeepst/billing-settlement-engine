package com.worldpay.pms.bse.engine.transformations.sources;

import static com.worldpay.pms.bse.engine.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.engine.BillingConfiguration;
import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.InputCorrectionEncodings;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillCorrectionRow;
import com.worldpay.pms.bse.engine.utils.WithDatabaseAndSpark;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.junit.SparkContext;
import com.worldpay.pms.testing.utils.DbUtils;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

public class BillCorrectionSourceTest implements WithDatabaseAndSpark {

  private static final String ROOT_DIR = "input/corrections/";

  private SqlDb cisadmDb;
  private SqlDb billingDb;
  private BillCorrectionSource billCorrectionSource;


  @Override
  public void bindBillingConfiguration(BillingConfiguration conf) {
    this.billCorrectionSource = new BillCorrectionSource(conf.getSources().getBillCorrectionConfiguration());
  }

  @Override
  public void bindBillingJdbcConfiguration(JdbcConfiguration conf) {
    this.billingDb = SqlDb.simple(conf);
  }

  @Override
  public void bindCisadmJdbcConfiguration(JdbcConfiguration conf) {
    this.cisadmDb = SqlDb.simple(conf);
  }

  @BeforeEach
  void cleanup() {
    DbUtils.cleanUpWithoutMetadata(cisadmDb, "cm_inv_recalc_stg");
    DbUtils.cleanUpWithoutMetadata(billingDb, "bill", "bill_line", "bill_line_calc", "bill_line_svc_qty");
  }

  @AfterAll
  static void afterAll() {
    SparkContext.reset();
  }

  @AfterEach
  void afterEach() {
    DbUtils.cleanUpWithoutMetadata(cisadmDb, "cm_inv_recalc_stg");
  }

  @Test
  void canReadEmptyDataset(SparkSession session) {
    LocalDateTime low = LocalDateTime.parse("2021-06-20T00:00:00");
    LocalDateTime high = LocalDateTime.parse("2021-06-21T00:00:00");
    BatchId batchId = createBatchId(low, high);

    Dataset<Tuple2<InputBillCorrectionRow, BillLineRow[]>> result = billCorrectionSource.load(session, batchId);
    assertThat(result.collectAsList().isEmpty(), is(true));
  }

  @Test
  void canReadNonNullCorrections(SparkSession session) {
    readFromCsvFilesAndWriteToExistingTables();

    LocalDateTime low = LocalDateTime.parse("2021-05-19T00:00:00");
    LocalDateTime high = LocalDateTime.parse("2021-05-20T00:00:00");
    BatchId batchId = createBatchId(low, high);

    Dataset<Tuple2<InputBillCorrectionRow, BillLineRow[]>> result = billCorrectionSource.load(session, batchId);

    InputBillCorrectionRow missingBill = result.filter(tuple -> tuple._2() == null)
        .map(Tuple2::_1, InputCorrectionEncodings.INPUT_BILL_CORRECTION_ROW_ENCODER)
        .collectAsList().get(0);

    assertThat(missingBill.getCorrectionEventId(), is("20000185357"));
    assertThat(missingBill.getBillId(), is("inexisting-bill"));

    BillRow bill = result.filter(tuple -> tuple._2() != null)
        .map(tuple -> BillRow.from(tuple._1, tuple._2()), Encodings.BILL_ROW_ENCODER)
        .collectAsList().get(0);

    assertThat(bill.getBillId(), is("Lk1T0@EpEavYj@g"));

    List<BillLineRow> billLineList = Arrays.asList(bill.getBillLines());
    assertThat(billLineList.size(), is(3));

    billLineList.forEach(line -> {
      assertThat(Arrays.asList(line.getBillLineCalculations()).size(), is(1));
      assertThat(Arrays.asList(line.getBillLineServiceQuantities()).size(), is(1));
    });
  }

  private void readFromCsvFilesAndWriteToExistingTables() {
    readFromCsvFileAndWriteToExistingTable(cisadmDb, ROOT_DIR + "cm_inv_recalc_stg.csv", "cm_inv_recalc_stg");
    readFromCsvFileAndWriteToExistingTable(billingDb, ROOT_DIR + "bill.csv", "bill");
    readFromCsvFileAndWriteToExistingTable(billingDb, ROOT_DIR + "bill_line.csv", "bill_line");
    readFromCsvFileAndWriteToExistingTable(billingDb, ROOT_DIR + "bill_line_calc.csv", "bill_line_calc");
    readFromCsvFileAndWriteToExistingTable(billingDb, ROOT_DIR + "bill_line_svc_qty.csv", "bill_line_svc_qty");
  }


}
