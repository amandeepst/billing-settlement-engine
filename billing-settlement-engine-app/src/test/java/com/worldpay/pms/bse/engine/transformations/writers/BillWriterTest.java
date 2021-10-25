package com.worldpay.pms.bse.engine.transformations.writers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.transformations.model.billprice.BillPriceRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetailRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
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

public class BillWriterTest extends JdbcWriterBaseTest<BillRow> {

  private static final BillLineServiceQuantityRow BILL_LINE_SVC_QTY = new BillLineServiceQuantityRow("TXN_AMT", BigDecimal.TEN, null);

  private static final BillLineCalculationRow BILL_LINE_CALC = new BillLineCalculationRow("billCalcId", "calcLnClass",
      "calcLnType", null, BigDecimal.ONE, "Y", "TXN_AMT", BigDecimal.ONE, "EXE", null,
      "taxDescription");

  private static final BillLineRow BILL_LINE_ROW = new BillLineRow("billLineId", "billLineParty", "PREMIUM", "ASFDG",
      "productDescription", "EUR", "EUR", BigDecimal.TEN, "EUR", BigDecimal.TEN, BigDecimal.ONE,
      "priceLineId", "merchantCode", BigDecimal.TEN, "EXE", null, new BillLineCalculationRow[]{BILL_LINE_CALC},
      new BillLineServiceQuantityRow[]{BILL_LINE_SVC_QTY});

  private static final BillRow BILL_ROW = new BillRow("billId", "partyId", "billSubAccountId",
      "tariffType", "templateType", "00001", "FUND", "accountId", "businessUnit", Date.valueOf(LocalDate.now()),
      "MONTHLY", Date.valueOf("2021-01-01"), Date.valueOf("2021-01-31"), "EUR", BigDecimal.TEN, "billRef", "COMPLETE",
      "N", "settSubLevelType", "settSubLevelValue", "granularity", "granularityKeyVal",
      "N", "N", "N", "N", "N", "N", null, null, null, null,
      null, null, null, null, null, null, null, null, null, new BillLineRow[]{BILL_LINE_ROW},
      new BillLineDetailRow[0], new BillPriceRow[0], null, (short) 0);

  @BeforeEach
  void cleanup() {
    DbUtils.cleanUp(db, "bill", "bill_line", "bill_line_calc", "bill_line_transposition", "bill_line_svc_qty");
  }

  @Test
  void canProcessNonEmptyPartition(SparkSession sparkSession) {
    long count = writer.write(batchId, NOW, sparkSession.createDataset(provideSamples(), encoder()));

    assertThat(count, is(1L));
    assertRowsWritten(1, 1, 1, 1);
  }

  @Override
  protected void assertNoRowsWritten() {
    assertRowsWritten(0, 0, 0, 0);
  }

  @Override
  protected List<BillRow> provideSamples() {
    return Arrays.asList(BILL_ROW);
  }

  @Override
  protected Encoder<BillRow> encoder() {
    return Encodings.BILL_ROW_ENCODER;
  }

  @Override
  protected JdbcWriter<BillRow> createWriter(JdbcWriterConfiguration jdbcWriterConfiguration) {
    return new BillWriter(jdbcWriterConfiguration);
  }

  private void assertRowsWritten(long expectedBillLineCount, long expectedBillLineSvcQtyCount, long expectedBillLineCalcCount,
      long expectedBillLineTranspositionCount) {
    assertCountIs("bill_line", expectedBillLineCount);
    assertCountIs("bill_line_svc_qty", expectedBillLineSvcQtyCount);
    assertCountIs("bill_line_calc", expectedBillLineCalcCount);
    assertCountIs("bill_line_transposition", expectedBillLineTranspositionCount);
  }
}
