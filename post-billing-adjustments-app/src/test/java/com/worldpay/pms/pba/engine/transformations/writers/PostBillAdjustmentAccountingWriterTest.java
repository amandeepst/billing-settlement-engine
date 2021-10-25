package com.worldpay.pms.pba.engine.transformations.writers;

import static com.worldpay.pms.pba.engine.transformations.writers.TestData.BILL_ADJUSTMENT_ACCT;
import static com.worldpay.pms.testing.utils.DbUtils.cleanUpWithoutMetadata;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.pba.engine.Encodings;
import com.worldpay.pms.pba.engine.model.output.BillAdjustmentAccountingRow;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import com.worldpay.pms.testing.junit.JdbcWriterBaseTest;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PostBillAdjustmentAccountingWriterTest extends JdbcWriterBaseTest<BillAdjustmentAccountingRow> {


  @BeforeEach
  void setUp() {
    cleanUpWithoutMetadata(db, "post_bill_adj_accounting");
  }

  @Test
  void canProcessNonEmptyPartition(SparkSession spark) {
    long count = write(spark.createDataset(provideSamples(), encoder()));

    assertThat(count, is(1L));
    assertCountIs("post_bill_adj_accounting", 1L);
  }

  @Override
  protected void assertNoRowsWritten() {
    assertCountIs("post_bill_adj_accounting", 0L);
  }

  @Override
  protected List<BillAdjustmentAccountingRow> provideSamples() {
    return Collections.singletonList(BILL_ADJUSTMENT_ACCT);
  }

  @Override
  protected Encoder<BillAdjustmentAccountingRow> encoder() {
    return Encodings.BILL_ADJUSTMENT_ACCOUNTING_ENCODER;
  }

  @Override
  protected JdbcWriter<BillAdjustmentAccountingRow> createWriter(JdbcWriterConfiguration jdbcWriterConfiguration) {
    return new PostBillAdjustmentAccountingWriter(jdbcWriterConfiguration);
  }
}
