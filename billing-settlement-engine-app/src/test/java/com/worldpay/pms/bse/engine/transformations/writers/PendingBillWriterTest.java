package com.worldpay.pms.bse.engine.transformations.writers;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetail;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import com.worldpay.pms.bse.engine.transformations.sources.PendingBillSourceTest;
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

class PendingBillWriterTest extends JdbcWriterBaseTest<PendingBillRow> {

  private static final List<String> SVC_QTY_CODES = Arrays.asList("txn_vol", "txn_amt", "ap_b_efx", "p_b_efx", "f_b_mfx", "f_m_amt");

  // this value is not used in writer (for now), the partitionId is populated with TaskContext.getPartitionId();
  private static final long partitionId = -1L;

  private static final PendingBillLineCalculationRow PENDING_BILL_LINE_CALC_1 = new PendingBillLineCalculationRow("bill_ln_calc_id_1",
      "class1", "type1", BigDecimal.valueOf(1.0), "Y", "rateType", BigDecimal.ONE, partitionId);
  private static final PendingBillLineCalculationRow PENDING_BILL_LINE_CALC_2 = new PendingBillLineCalculationRow("bill_ln_calc_id_2",
      "class2", "type2", BigDecimal.valueOf(1.0), "Y", "rateType", BigDecimal.ONE, partitionId);
  private static final PendingBillLineCalculationRow PENDING_BILL_LINE_CALC_3 = new PendingBillLineCalculationRow("bill_ln_calc_id_3",
      "class3", "type3", BigDecimal.valueOf(1.0), "Y", "rateType", BigDecimal.ONE, partitionId);
  private static final PendingBillLineCalculationRow PENDING_BILL_LINE_CALC_4 = new PendingBillLineCalculationRow("bill_ln_calc_id_4",
      "class4", "type3", BigDecimal.valueOf(1.0), "Y", "rateType", BigDecimal.ONE, partitionId);
  private static final PendingBillLineCalculationRow PENDING_BILL_LINE_CALC_5 = new PendingBillLineCalculationRow("bill_ln_calc_id_5",
      "class5", "type3", BigDecimal.valueOf(1.0), "Y", "rateType", BigDecimal.ONE, partitionId);

  private static final PendingBillLineRow PENDING_BILL_LINE_1 = new PendingBillLineRow("bill_ln_id_1", "P0001001",
      "prod_class_1", "prod_id_1", "EUR", "EUR", BigDecimal.valueOf(2.0), "EUR", BigDecimal.valueOf(5.0), BigDecimal.valueOf(1.0),
      "price_ln_id_1", "merchant_code_1", partitionId, BillLineDetail.array("bill_item_id1", "1234"),
      new PendingBillLineCalculationRow[]{PENDING_BILL_LINE_CALC_1, PENDING_BILL_LINE_CALC_2},
      generatePendingBillLineSvcQtyArray());
  private static final PendingBillLineRow PENDING_BILL_LINE_2 = new PendingBillLineRow("bill_ln_id_2", "P0001001",
      "prod_class_1", "prod_id_1", "EUR", "EUR", BigDecimal.valueOf(1.0), "EUR", BigDecimal.valueOf(5.0), BigDecimal.valueOf(1.0),
      "price_ln_id_1", "merchant_code_1", partitionId, BillLineDetail.array("bill_item_id2", "4321"),
      new PendingBillLineCalculationRow[]{PENDING_BILL_LINE_CALC_3},
      generatePendingBillLineSvcQtyArray());
  private static final PendingBillLineRow PENDING_BILL_LINE_3 = new PendingBillLineRow("bill_ln_id_3", "P0001001",
      "prod_class_1", "prod_id_1", "EUR", "EUR", BigDecimal.valueOf(2.0), "EUR", BigDecimal.valueOf(5.0), BigDecimal.valueOf(1.0),
      "price_ln_id_1", "merchant_code_1", partitionId, BillLineDetail.array("bill_item_id3", "2341"),
      new PendingBillLineCalculationRow[]{PENDING_BILL_LINE_CALC_4, PENDING_BILL_LINE_CALC_5},
      generatePendingBillLineSvcQtyArray());

  private static final PendingBillRow PENDING_BILL_1 = new PendingBillRow("bill_id_1", "P0001001", "lcp_1", "acct1", "bsa_1", "FUND", "B1",
      "BC1", Date.valueOf("2020-11-10"), Date.valueOf("2020-11-10"), "EUR", "BR1", "N", "SSLT1", "SSLV", "G1", "GKV",
      Date.valueOf("2020-11-10"), "debtMigrationType1", "N", "N", "N", "N", "N", "N", "N", "Y",
      Date.valueOf("2020-11-10"), (short) 0, partitionId, new PendingBillLineRow[]{PENDING_BILL_LINE_1, PENDING_BILL_LINE_2},
      null, null, null, null, 0);
  private static final PendingBillRow PENDING_BILL_2 = new PendingBillRow("bill_id_2", "P0001001", "lcp_1", "acct1", "bsa_1", "FUND", "B1",
      "BC1", Date.valueOf("2020-11-10"), Date.valueOf("2020-11-10"), "EUR", "BR1", "N", "SSLT1", "SSLV", "G1", "GKV",
      Date.valueOf("2020-11-10"), "debtMigrationType1", "N", "N", "N", "N", "N", "N", "N", "N",
      Date.valueOf("2020-11-10"), (short) 0, partitionId, new PendingBillLineRow[]{PENDING_BILL_LINE_3},
      null, null,null,  null, 0);

  private static PendingBillLineServiceQuantityRow[] generatePendingBillLineSvcQtyArray() {
    return SVC_QTY_CODES.stream()
        .map(code -> new PendingBillLineServiceQuantityRow(code, BigDecimal.ONE, partitionId))
        .toArray(PendingBillLineServiceQuantityRow[]::new);
  }

  @BeforeEach
  void setUp() {
    DbUtils.cleanUp(db, "bill_line_detail", "pending_bill_line_svc_qty", "pending_bill_line_calc", "pending_bill_line", "pending_bill");
  }

  @Test
  void canWriteNonEmptyPartition(SparkSession spark) {
    List<PendingBillRow> pendingBills = provideSamples();
    pendingBills.forEach(PendingBillSourceTest::assertConstraintsOnFieldsForPendingBill);

    write(spark.createDataset(pendingBills, encoder()));

    assertRowsWritten(2, 3, 5, 18);
  }

  @Test
  void whenDataIsLoadedThenVirtualColumnPartitionIsCorrectlyComputed(SparkSession spark) {
    write(spark.createDataset(provideSamples(), encoder()));

    assertTrue(isWithinDesignedBoundaries(getPartitionsFromTable("pending_bill"), 0, 63));
    assertTrue(isWithinDesignedBoundaries(getPartitionsFromTable("pending_bill_line"), 0, 127));
    assertTrue(isWithinDesignedBoundaries(getPartitionsFromTable("pending_bill_line_calc"), 0, 255));
    assertTrue(isWithinDesignedBoundaries(getPartitionsFromTable("pending_bill_line_svc_qty"), 0, 255));
  }

  private List<Long> getPartitionsFromTable(String table) {
    return db.execQuery("get partition column from " + table, "SELECT partition FROM " + table, (query) ->
        query.executeAndFetch(Long.class));
  }

  private boolean isWithinDesignedBoundaries(List<Long> values, long minVal, long maxVal) {
    return values.stream().noneMatch(v -> v < minVal || v > maxVal);
  }

  private void assertRowsWritten(long expectedPendingBillCount, long expectedPendingBillLineCount, long expectedPendingBillLineCalcCount,
      long expectedPendingBillLineSvcQtyCount) {
    assertCountIs("pending_bill", expectedPendingBillCount);
    assertCountIs("pending_bill_line", expectedPendingBillLineCount);
    assertCountIs("pending_bill_line_calc", expectedPendingBillLineCalcCount);
    assertCountIs("pending_bill_line_svc_qty", expectedPendingBillLineSvcQtyCount);
    // PendingBillWriter should not write to bill_line_detail table
    assertCountIs("bill_line_detail", 0);
  }

  @Override
  protected void assertNoRowsWritten() {
    assertRowsWritten(0, 0, 0, 0);
  }

  @Override
  protected List<PendingBillRow> provideSamples() {
    return Arrays.asList(PENDING_BILL_1, PENDING_BILL_2);
  }

  @Override
  protected Encoder<PendingBillRow> encoder() {
    return Encodings.PENDING_BILL_ENCODER;
  }

  @Override
  protected JdbcWriter<PendingBillRow> createWriter(JdbcWriterConfiguration jdbcWriterConfiguration) {
    return new PendingBillWriter(jdbcWriterConfiguration);
  }
}