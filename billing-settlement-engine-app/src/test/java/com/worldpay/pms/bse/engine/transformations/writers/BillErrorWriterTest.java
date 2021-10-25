package com.worldpay.pms.bse.engine.transformations.writers;

import static org.apache.spark.sql.SparkSession.getActiveSession;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.transformations.model.FieldConstants;
import com.worldpay.pms.bse.engine.transformations.model.billerror.BillError;
import com.worldpay.pms.bse.engine.transformations.model.billerror.BillErrorDetail;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import com.worldpay.pms.pce.common.DomainError;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import com.worldpay.pms.testing.junit.JdbcWriterBaseTest;
import com.worldpay.pms.testing.junit.SparkContext;
import com.worldpay.pms.testing.stereotypes.WithSpark;
import com.worldpay.pms.testing.utils.DbUtils;
import java.sql.Date;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@WithSpark
class BillErrorWriterTest extends JdbcWriterBaseTest<BillError> {

  private static final PendingBillRow PENDING_BILL_1 = new PendingBillRow("bill_id_1", "P0001001", "lcp_1", "acct1", "bsa_1", "FUND", "B1",
      "WPMO", Date.valueOf("2020-11-10"), Date.valueOf("2020-11-10"), "EUR", "BR1", "N", "SSLT1", "SSLV", "G1", "GKV",
      Date.valueOf("2020-11-10"), "debtMigrationType1", "N", "N", "N", "N", "N", "N", "N", "N", Date.valueOf("2020-11-12"), (short) 0, 0, null,
      null, null, null, null, 0);
  private static final PendingBillRow PENDING_BILL_2 = new PendingBillRow("bill_id_2", "P0001002", "lcp_1", "acct1", "bsa_1", "FUND", "B1",
      "WPMO", Date.valueOf("2020-11-10"), Date.valueOf("2020-11-10"), "EUR", "BR1", "N", "SSLT1", "SSLV", "G1", "GKV",
      Date.valueOf("2020-11-10"), "debtMigrationType1", "N", "N", "N", "N", "N", "N", "N", "N", Date.valueOf("2020-11-12"), (short) 0, 0, null,
      null, null, null, null, 0);
  private static final PendingBillRow PENDING_BILL_TO_FAIL_TODAY = new PendingBillRow("bill_id_3", "P0001001", "lcp_1", "acct1", "bsa_1",
      "FUND", "B1", "WPMO", Date.valueOf("2020-11-10"), Date.valueOf("2020-11-10"), "EUR", "BR1", "N", "SSLT1", "SSLV", "G1", "GKV",
      Date.valueOf("2020-11-10"), "debtMigrationType1", "N", "N", "N", "N", "N", "N", "N", "Y", new Date(System.currentTimeMillis()), (short) 0,
      0, null, null, null, null, null, 0);

  @AfterAll
  static void afterAll() {
    SparkContext.reset();
  }

  @BeforeEach
  void setUp() {
    DbUtils.cleanUp(db, "bill_error_detail", "bill_error");
  }

  @Test
  void canWriteNonEmptyPartition(SparkSession spark) {
    write(spark.createDataset(provideSamples(), encoder()));

    assertRowsWritten(3, 3);
    assertAccumulatorsAreValid(1, 0);
  }

  @Override
  protected void assertNoRowsWritten() {
    assertRowsWritten(0, 0);
  }

  @Override
  protected List<BillError> provideSamples() {

    BillErrorDetail billErrorDetail1 = BillErrorDetail.of("bill_ln_id_1", new DomainError("error_code", "error_message"));
    BillErrorDetail billErrorDetail2 = BillErrorDetail.of("bill_ln_id_2", new RuntimeException("exception message"));

    BillError billError1 = BillError.error(
        PENDING_BILL_1,
        new BillErrorDetail[]{billErrorDetail2});
    BillError billError2 = BillError.error(
        PENDING_BILL_2,
        null);
    BillError billError3 = BillError.error(
        PENDING_BILL_TO_FAIL_TODAY,
        new BillErrorDetail[]{billErrorDetail1, billErrorDetail2});

    return Arrays.asList(billError1, billError2, billError3);
  }

  @Override
  protected Encoder<BillError> encoder() {
    return Encodings.BILL_ERROR_ENCODER;
  }

  @Override
  protected JdbcWriter<BillError> createWriter(JdbcWriterConfiguration jdbcWriterConfiguration) {
    return new BillErrorWriter(jdbcWriterConfiguration, getActiveSession().get());
  }

  private void assertRowsWritten(long expectedBillErrorCount, long expectedBillErrorDetailCount) {
    assertCountIs("bill_error", expectedBillErrorCount);
    assertCountIs("bill_error_detail", expectedBillErrorDetailCount);
  }

  private void assertAccumulatorsAreValid(long failedTodayErrors, long ignoredErrors) {
    assertTrue(writer instanceof BillErrorWriter);
    BillErrorWriter billErrorWriter = (BillErrorWriter) writer;
    assertEquals(failedTodayErrors, billErrorWriter.getFirstFailureCount());
    assertEquals(ignoredErrors, billErrorWriter.getIgnoredCount());
  }

  @Test
  void testIgnoredEventsAreCountedCorrectly(SparkSession spark) {

    io.vavr.collection.List<BillError> samples = io.vavr.collection.List.ofAll(provideSamples());

    for (int i = 0; i < 57; i++) {
      samples = samples.append(BillError
          .error("id" + i, Date.valueOf("2021-09-09"), new Date(System.currentTimeMillis()), FieldConstants.IGNORED_RETRY_COUNT,
              new BillErrorDetail[]{}));
    }

    write(spark.createDataset(samples.asJava(), encoder()));

    assertRowsWritten(60, 3);
    assertAccumulatorsAreValid(1, 57);
  }
}