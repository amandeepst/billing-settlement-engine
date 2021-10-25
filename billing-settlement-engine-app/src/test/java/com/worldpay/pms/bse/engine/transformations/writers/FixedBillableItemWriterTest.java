package com.worldpay.pms.bse.engine.transformations.writers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.transformations.model.failedbillableitem.FailedBillableItem;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import com.worldpay.pms.testing.junit.JdbcWriterBaseTest;
import com.worldpay.pms.testing.utils.DbUtils;
import java.sql.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FixedBillableItemWriterTest extends JdbcWriterBaseTest<FailedBillableItem> {

  private static final String ERROR_TABLE = "bill_item_error";

  private static final PendingBillRow PENDING_BILL_1 = new PendingBillRow("bill_id_1", "P0001001", "lcp_1", "acct1", "bsa_1", "FUND", "B1",
      "WPMO", Date.valueOf("2020-11-10"), Date.valueOf("2020-11-10"), "EUR", "BR1", "N", "SSLT1", "SSLV", "G1", "GKV",
      Date.valueOf("2020-11-10"), "debtMigrationType1", "N", "N", "N", "N", "N", "N", "N", "N", Date.valueOf("2020-11-12"), (short) 0, 0, null,
      null, "bill_id_1", Date.valueOf("2021-01-10"), Date.valueOf("2021-01-10"), 3);
  private static final PendingBillRow PENDING_BILL_2 = new PendingBillRow("bill_id_2", "P0001002", "lcp_1", "acct1", "bsa_1", "FUND", "B1",
      "WPMO", Date.valueOf("2020-11-10"), Date.valueOf("2020-11-10"), "EUR", "BR1", "N", "SSLT1", "SSLV", "G1", "GKV",
      Date.valueOf("2020-11-10"), "debtMigrationType1", "N", "N", "N", "N", "N", "N", "N", "N", Date.valueOf("2020-11-12"), (short) 0, 0, null,
      null, "bill_id_2", Date.valueOf("2021-01-15"), Date.valueOf("2021-01-15"), 2);
  private static final PendingBillRow PENDING_BILL_3 = new PendingBillRow("bill_id_3", "P0001001", "lcp_1", "acct1", "bsa_1",
      "FUND", "B1", "WPMO", Date.valueOf("2020-11-10"), Date.valueOf("2020-11-10"), "EUR", "BR1", "N", "SSLT1", "SSLV", "G1", "GKV",
      Date.valueOf("2020-11-10"), "debtMigrationType1", "N", "N", "N", "N", "N", "N", "N", "N", new Date(System.currentTimeMillis()), (short) 0,
      0, null, null, "bill_id_3", Date.valueOf("2020-02-10"), Date.valueOf("2021-02-10"), 1);

  @BeforeEach
  void setUp() {
    DbUtils.cleanUp(db, ERROR_TABLE);
  }

  @Test
  void canWriteNonEmptyPartition(SparkSession spark) {
    write(spark.createDataset(provideSamples(), encoder()));

    assertRowsWritten(3L);
  }

  private void assertRowsWritten(long expectedFailedBillableItemCount) {
    assertThat(getFailedBillableItemsCount(), is(expectedFailedBillableItemCount));
  }

  private long getFailedBillableItemsCount() {
    return DbUtils.getCount(db, ERROR_TABLE);
  }

  @Override
  protected void assertNoRowsWritten() {
    assertRowsWritten(0);
  }

  @Override
  protected List<FailedBillableItem> provideSamples() {
    return Stream.of(PENDING_BILL_1, PENDING_BILL_2, PENDING_BILL_3)
        .map(FailedBillableItem::fixed)
        .collect(Collectors.toList());
  }

  @Override
  protected Encoder<FailedBillableItem> encoder() {
    return Encodings.FAILED_BILLABLE_ITEM_ENCODER;
  }

  @Override
  protected JdbcWriter<FailedBillableItem> createWriter(JdbcWriterConfiguration jdbcWriterConfiguration) {
    return new FixedFailedBillableItemWriter(jdbcWriterConfiguration);
  }

}