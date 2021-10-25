package com.worldpay.pms.bse.engine.transformations.writers;


import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.engine.InputEncodings;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.PendingMinChargeRow;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import com.worldpay.pms.testing.junit.JdbcWriterBaseTest;
import com.worldpay.pms.testing.utils.DbUtils;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sql2o.Query;
import org.sql2o.Sql2oQuery;

class PendingMinimumChargeWriterTest extends JdbcWriterBaseTest<PendingMinChargeRow> {

  private static final String QUERY_COUNT_PENDING_MIN_CHARGE_BY_ALL_FIELDS = resourceAsString(
      "sql/min_charge/count_pending_min_charge_by_all_fields.sql");

  private static final PendingMinChargeRow PENDING_MINIMUM_CHARGE = PendingMinChargeRow.builder()
      .billPartyId("PO12345")
      .legalCounterparty("PO1100100002")
      .txnPartyId("PO00001")
      .minChargeStartDate(Date.valueOf("2021-03-01"))
      .minChargeEndDate(Date.valueOf("2021-03-31"))
      .minChargeType("min_chg_type")
      .applicableCharges(BigDecimal.TEN)
      .currency("GBP")
      .billDate(Date.valueOf("2021-03-15"))
      .build();

  @BeforeEach
  void setUp() {
    DbUtils.cleanUp(db, "pending_min_charge");
  }

  @Override
  protected void assertNoRowsWritten() {
    assertRowsWritten(emptyList());
  }

  @Test
  void canWriteNonEmptyPartition(SparkSession spark) {
    List<PendingMinChargeRow> samples = provideSamples();
    write(spark.createDataset(samples, encoder()));
    assertRowsWritten(samples);
  }

  @Override
  protected List<PendingMinChargeRow> provideSamples() {
    return Arrays.asList(
        PENDING_MINIMUM_CHARGE,
        PENDING_MINIMUM_CHARGE.withBillPartyId("PO23456").withTxnPartyId("PO00002"),
        PENDING_MINIMUM_CHARGE.withBillPartyId("PO34567").withTxnPartyId("PO00003"),
        PENDING_MINIMUM_CHARGE.withBillPartyId("PO45678").withTxnPartyId("PO00002"),
        PENDING_MINIMUM_CHARGE.withBillPartyId("PO56789").withTxnPartyId("PO00003")
    );
  }

  @Override
  protected Encoder<PendingMinChargeRow> encoder() {
    return InputEncodings.PENDING_MIN_CHARGE_ROW_ENCODER;
  }

  @Override
  protected JdbcWriter<PendingMinChargeRow> createWriter(JdbcWriterConfiguration jdbcWriterConfiguration) {
    return new PendingMinimumChargeWriter(jdbcWriterConfiguration);
  }

  private void assertRowsWritten(List<PendingMinChargeRow> rows) {
    assertCountIs("pending_min_charge", rows.size());
    rows.forEach(row -> {
      long count = db.execQuery("count-pending-min-charge-by-all-fields", QUERY_COUNT_PENDING_MIN_CHARGE_BY_ALL_FIELDS, (query) ->
          getQuery(query, row).executeScalar(Long.TYPE));
      assertThat(count, is(1L));
    });
  }

  private Query getQuery(Sql2oQuery query, PendingMinChargeRow row) {
    return query
        .addParameter("bill_party_id", row.getBillPartyId())
        .addParameter("legal_counterparty", row.getLegalCounterparty())
        .addParameter("txn_party_id", row.getTxnPartyId())
        .addParameter("min_chg_start_dt", row.getMinChargeStartDate())
        .addParameter("min_chg_end_dt", row.getMinChargeEndDate())
        .addParameter("min_chg_type", row.getMinChargeType())
        .addParameter("applicable_charges", row.getApplicableCharges())
        .addParameter("currency", row.getCurrency())
        .addParameter("bill_dt", row.getBillDate())
        .addParameter("batch_code", batchId.code)
        .addParameter("batch_attempt", batchId.attempt)
        .addParameter("ilm_dt", Timestamp.valueOf(NOW))
        .addParameter("ilm_arch_sw", "Y");
  }
}