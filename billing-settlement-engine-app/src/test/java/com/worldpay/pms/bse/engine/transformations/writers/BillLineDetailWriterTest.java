package com.worldpay.pms.bse.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetailRow;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import com.worldpay.pms.testing.junit.JdbcWriterBaseTest;
import com.worldpay.pms.testing.utils.DbUtils;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sql2o.Query;
import org.sql2o.Sql2oQuery;

class BillLineDetailWriterTest extends JdbcWriterBaseTest<BillLineDetailRow> {

  private static final String ROOT = "sql/bill_line_detail/";
  private static final String QUERY_COUNT_BILL_LINE_DETAIL_BY_ALL_FIELDS = resourceAsString(ROOT + "count_bill_line_detail_by_all_fields.sql");

  private static final BillLineDetailRow BILL_LINE_DETAIL_1 = new BillLineDetailRow("bill_id_1", "bill_ln_id_11","bill_item_id1", "1234");
  private static final BillLineDetailRow BILL_LINE_DETAIL_2 = new BillLineDetailRow("bill_id_1", "bill_ln_id_12","bill_item_id2", "4321");
  private static final BillLineDetailRow BILL_LINE_DETAIL_3 = new BillLineDetailRow("bill_id_2", "bill_ln_id_21","bill_item_id3", "2341");

  @BeforeEach
  void setUp() {
    DbUtils.cleanUp(db, "bill_line_detail");
  }

  @Test
  void canWriteNonEmptyPartition(SparkSession spark) {
    List<BillLineDetailRow> samples = provideSamples();
    write(spark.createDataset(samples, encoder()));
    assertRowsWritten(samples);
  }

  @Test
  void whenDataIsLoadedThenVirtualColumnPartitionIsCorrectlyComputed(SparkSession spark) {
    write(spark.createDataset(provideSamples(), encoder()));
    assertTrue(isWithinDesignedBoundaries(getPartitionsFromTable("bill_line_detail"), 0, 127));
  }

  @Override
  protected void assertNoRowsWritten() {
    assertRowsWritten(emptyList());
  }

  @Override
  protected List<BillLineDetailRow> provideSamples() {
    return Arrays.asList(BILL_LINE_DETAIL_1, BILL_LINE_DETAIL_2, BILL_LINE_DETAIL_3);
  }

  @Override
  protected Encoder<BillLineDetailRow> encoder() {
    return Encodings.BILL_LINE_DETAIL_ROW_ENCODER;
  }

  @Override
  protected JdbcWriter<BillLineDetailRow> createWriter(JdbcWriterConfiguration jdbcWriterConfiguration) {
    return new BillLineDetailWriter(jdbcWriterConfiguration);
  }

  private void assertRowsWritten(List<BillLineDetailRow> rows) {
    assertCountIs("bill_line_detail", rows.size());
    rows.forEach(row -> {
      long count = db.execQuery("count-bill-line-detail-by-all-fields", QUERY_COUNT_BILL_LINE_DETAIL_BY_ALL_FIELDS, (query) ->
          getQuery(query, row).executeScalar(Long.TYPE));
      assertThat(count, is(1L));
    });
  }

  private List<Long> getPartitionsFromTable(String table) {
    return db.execQuery("get partition column from " + table, "SELECT partition FROM " + table, (query) ->
        query.executeAndFetch(Long.class));
  }

  private boolean isWithinDesignedBoundaries(List<Long> values, long minVal, long maxVal) {
    return values.stream().noneMatch(v -> v < minVal || v > maxVal);
  }

  private Query getQuery(Sql2oQuery query, BillLineDetailRow row) {
    return query
        .addParameter("bill_item_id", row.getBillItemId())
        .addParameter("bill_item_hash", row.getBillItemHash())
        .addParameter("bill_id", row.getBillId())
        .addParameter("bill_ln_id", row.getBillLineId())
        .addParameter("batch_code", batchId.code)
        .addParameter("batch_attempt", batchId.attempt)
        .addParameter("ilm_dt", Timestamp.valueOf(NOW))
        .addParameter("ilm_arch_sw", "Y");
  }
}