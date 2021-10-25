package com.worldpay.pms.bse.engine.transformations.writers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.transformations.model.billerror.BillError;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import com.worldpay.pms.testing.junit.JdbcWriterBaseTest;
import com.worldpay.pms.testing.utils.DbUtils;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FixedBillErrorWriterTest extends JdbcWriterBaseTest<BillError> {

  private static final BillRow BILL_1 = new BillRow("bill_id_1", "P0001001", "bsa_1", null, null, "lcp_1", "FUND", "acct1", "B1",
      Date.valueOf("2020-11-10"), "WPMO", Date.valueOf("2020-11-10"), Date.valueOf("2020-11-10"), "EUR", BigDecimal.ZERO, "BR1", "STATUS", "N", "SSLT1", "SSLV", "G1", "GKV",
      "N", "N", "N", "N", "N", "N", null, null, Date.valueOf("2020-11-10"), "debtMigrationType1", null, null, null, null, null, null, null, null, null, null, null, null,
      Date.valueOf("2020-11-11"), (short) 2);
  private static final BillRow BILL_2 = new BillRow("bill_id_2", "P0001001", "bsa_1", null, null, "lcp_1", "FUND", "acct1", "B1",
      Date.valueOf("2020-11-10"), "WPMO", Date.valueOf("2020-11-10"), Date.valueOf("2020-11-10"), "EUR", BigDecimal.ZERO, "BR1", "STATUS", "N", "SSLT1", "SSLV", "G1", "GKV",
      "N", "N", "N", "N", "N", "N", null, null, Date.valueOf("2020-11-10"), "debtMigrationType1", null, null, null, null, null, null, null, null, null, null, null, null,
      Date.valueOf("2020-11-12"), (short) 2);

  @BeforeEach
  void setUp() {
    DbUtils.cleanUp(db, "bill_error");
  }

  @Test
  void canWriteNonEmptyPartition(SparkSession spark) {
    long count = write(spark.createDataset(provideSamples(), encoder()));

    assertEquals(2, count);
    assertRowsWritten(2);
  }

  @Override
  protected void assertNoRowsWritten() {
    assertRowsWritten(0);
  }

  @Override
  protected List<BillError> provideSamples() {

    BillError billError1 = BillError.fixed(BILL_1);
    BillError billError2 = BillError.fixed(BILL_2);

    return Arrays.asList(billError1, billError2);
  }

  @Override
  protected Encoder<BillError> encoder() {
    return Encodings.BILL_ERROR_ENCODER;
  }

  @Override
  protected JdbcWriter<BillError> createWriter(JdbcWriterConfiguration jdbcWriterConfiguration) {
    return new FixedBillErrorWriter(jdbcWriterConfiguration);
  }

  private void assertRowsWritten(long expectedBillErrorCount) {
    assertCountIs("bill_error", expectedBillErrorCount);
  }
}