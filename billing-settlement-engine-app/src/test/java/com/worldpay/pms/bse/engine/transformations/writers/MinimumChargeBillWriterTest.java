package com.worldpay.pms.bse.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.engine.InputEncodings;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.MinimumChargeBillRow;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import com.worldpay.pms.testing.junit.JdbcWriterBaseTest;
import com.worldpay.pms.testing.utils.DbUtils;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Encoder;
import org.junit.jupiter.api.BeforeEach;
import org.sql2o.Query;
import org.sql2o.Sql2oQuery;

public class MinimumChargeBillWriterTest extends JdbcWriterBaseTest<MinimumChargeBillRow> {

  private static final String QUERY_COUNT_MIN_CHARGE_BILLS_BY_ALL_FIELDS = resourceAsString(
      "sql/min_charge/count_pending_min_charge_by_all_fields.sql");

  private static final MinimumChargeBillRow.MinimumChargeBillRowBuilder MINIMUM_CHARGE_BILLS = MinimumChargeBillRow.builder()
      .billPartyId("PO12345")
      .legalCounterparty("PO1100100002")
      .currency("GBP")
      .logicalDate(Date.valueOf("2021-03-15"));

  @BeforeEach
  void setUp() {
    DbUtils.cleanUp(db, "minimum_charge_bill");
  }

  @Override
  protected void assertNoRowsWritten() {
    assertRowsWritten(emptyList());
  }

  @Override
  protected List<MinimumChargeBillRow> provideSamples() {
    return Arrays.asList(
        MINIMUM_CHARGE_BILLS.build(),
        MINIMUM_CHARGE_BILLS.billPartyId("PO23456").build(),
        MINIMUM_CHARGE_BILLS.billPartyId("PO34567").build(),
        MINIMUM_CHARGE_BILLS.billPartyId("PO45678").build(),
        MINIMUM_CHARGE_BILLS.billPartyId("PO56789").build()
    );
  }

  @Override
  protected Encoder<MinimumChargeBillRow> encoder() {
    return InputEncodings.MIN_CHARGE_BILL_ROW_ENCODER;
  }

  @Override
  protected JdbcWriter<MinimumChargeBillRow> createWriter(JdbcWriterConfiguration jdbcWriterConfiguration) {
    return new MinChargeBillsWriter(jdbcWriterConfiguration);
  }

  private void assertRowsWritten(List<MinimumChargeBillRow> rows) {
    assertCountIs("minimum_charge_bill", rows.size());
    rows.forEach(row -> {
      long count = db.execQuery("count-min-charge-bills-by-all-fields", QUERY_COUNT_MIN_CHARGE_BILLS_BY_ALL_FIELDS, (query) ->
          getQuery(query, row).executeScalar(Long.TYPE));
      assertThat(count, is(1L));
    });
  }

  private Query getQuery(Sql2oQuery query, MinimumChargeBillRow row) {
    return query
        .addParameter("bill_party_id", row.getBillPartyId())
        .addParameter("legal_counterparty", row.getLegalCounterparty())
        .addParameter("currency", row.getCurrency())
        .addParameter("logical_date", row.getLogicalDate())
        .addParameter("batch_code", batchId.code)
        .addParameter("batch_attempt", batchId.attempt)
        .addParameter("ilm_dt", Timestamp.valueOf(NOW))
        .addParameter("ilm_arch_sw", "Y");
  }
}
