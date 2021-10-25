package com.worldpay.pms.bse.engine.transformations.writers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.transformations.model.billaccounting.BillAccountingRow;
import com.worldpay.pms.config.ApplicationConfiguration;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import com.worldpay.pms.testing.junit.JdbcWriterBaseTest;
import com.worldpay.pms.testing.utils.DbUtils;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BillAccountingWriterTest extends JdbcWriterBaseTest<BillAccountingRow> {

  public static final BillAccountingRow BILL_ACCOUNTING_ROW = new BillAccountingRow("partyId", "accountId", "accountType",
      "billSubAccountId", "subAccountType", "EUR", BigDecimal.TEN, "123456789~", "09734h83d", BigDecimal.TEN, "00001",
      "businessUnit", "PREMIUM", "iwghs98h3g2`", "type", "calcType", BigDecimal.ONE, Date.valueOf(LocalDate.now()));

  @BeforeEach
  void setUp() {
    DbUtils.cleanUp(db, "bill_accounting");
  }

  @Test
  void canProcessNonEmptyPartition(ApplicationConfiguration conf, SparkSession spark) {
    long count = write(spark.createDataset(provideSamples(), encoder()));

    assertThat(count, is(1L));
    assertCountIs("bill_accounting", 1L);
  }

  @Override
  protected void assertNoRowsWritten() {
    assertCountIs("bill_accounting", 0L);
  }

  @Override
  protected List<BillAccountingRow> provideSamples() {
    return Arrays.asList(BILL_ACCOUNTING_ROW);
  }

  @Override
  protected Encoder<BillAccountingRow> encoder() {
    return Encodings.BILL_ACCOUNTING_ROW_ENCODER;
  }

  @Override
  protected JdbcWriter<BillAccountingRow> createWriter(JdbcWriterConfiguration jdbcWriterConfiguration) {
    return new BillAccountingWriter(jdbcWriterConfiguration);
  }


}
