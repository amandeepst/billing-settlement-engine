package com.worldpay.pms.pba.engine.transformations.writers;

import static com.worldpay.pms.pba.engine.transformations.writers.TestData.BILL_ADJUSTMENT_1;
import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.testing.utils.DbUtils.cleanUpWithoutMetadata;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.pba.engine.Encodings;
import com.worldpay.pms.pba.domain.model.BillAdjustment;
import com.worldpay.pms.pba.engine.PostBillingConfiguration.PostBillAdjustmentWriterConfiguration;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import com.worldpay.pms.testing.junit.JdbcWriterBaseTest;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

class PostBillAdjustmentWriterTest extends JdbcWriterBaseTest<Tuple2<String, BillAdjustment>> {

  static final String QUERY_COUNT_POST_BILL_ADJ_BY_ALL_FIELDS = resourceAsString("sql/post_bill_adj/count_post_bill_adj_by_all_fields.sql");

  @BeforeEach
  void setUp() {
    cleanUpWithoutMetadata(db, "post_bill_adj");
  }

  @Test
  void canProcessNonEmptyPartition(SparkSession spark) {
    long writerCount = write(spark.createDataset(provideSamples(), encoder()));
    assertThat(writerCount, is(1L));

    long dbCount = db.execQuery("count-post-bill-adj-by-all-fields", QUERY_COUNT_POST_BILL_ADJ_BY_ALL_FIELDS, (query) ->
        query.addParameter("post_bill_adj_id", "1")
            .addParameter("post_bill_adj_type", BILL_ADJUSTMENT_1.getAdjustment().getType())
            .addParameter("post_bill_adj_descr", BILL_ADJUSTMENT_1.getAdjustment().getDescription())
            .addParameter("bill_id", BILL_ADJUSTMENT_1.getBill().getBillId())
            .addParameter("amount", BILL_ADJUSTMENT_1.getAdjustment().getAmount())
            .addParameter("currency_cd", BILL_ADJUSTMENT_1.getBill().getCurrencyCode())
            .addParameter("bill_sub_acct_id", BILL_ADJUSTMENT_1.getBill().getBillSubAccountId())
            .addParameter("rel_sub_acct_id", BILL_ADJUSTMENT_1.getAdjustment().getSubAccountId())
            .addParameter("party_id", BILL_ADJUSTMENT_1.getBill().getPartyId())
            .addParameter("bill_ref", BILL_ADJUSTMENT_1.getBill().getBillReference())
            .addParameter("granularity", BILL_ADJUSTMENT_1.getBill().getGranularity())
            .addParameter("acct_type", BILL_ADJUSTMENT_1.getBill().getAccountType())
            .addParameter("bill_dt", BILL_ADJUSTMENT_1.getBill().getBillDate())
            .addParameter("batch_code", batchId.code)
            .addParameter("batch_attempt", batchId.attempt)
            .addParameter("partition_id", 0)
            .addParameter("ilm_dt", Timestamp.valueOf(NOW))
            .addParameter("ilm_arch_sw", "Y")
            .executeScalar(Long.TYPE));

    assertThat(dbCount, is(1L));
  }

  @Override
  protected void assertNoRowsWritten() {
    assertCountIs("post_bill_adj", 0L);
  }

  @Override
  protected List<Tuple2<String, BillAdjustment>> provideSamples() {
    return Collections.singletonList(new Tuple2<>("1", BILL_ADJUSTMENT_1));
  }

  @Override
  protected Encoder<Tuple2<String, BillAdjustment>> encoder() {
    return Encodings.BILL_ADJUSTMENT_WITH_ID_ENCODER;
  }

  @Override
  protected JdbcWriter<Tuple2<String, BillAdjustment>> createWriter(JdbcWriterConfiguration jdbcWriterConfiguration) {
    PostBillAdjustmentWriterConfiguration postBillAdjustmentWriterConfiguration = new PostBillAdjustmentWriterConfiguration();
    postBillAdjustmentWriterConfiguration.setDataSource(jdbcWriterConfiguration.getDataSource());
    postBillAdjustmentWriterConfiguration.setEnabled(jdbcWriterConfiguration.isEnabled());
    postBillAdjustmentWriterConfiguration.setHints(jdbcWriterConfiguration.getHints());
    postBillAdjustmentWriterConfiguration.setIdempotencyEnabled(jdbcWriterConfiguration.isIdempotencyEnabled());
    postBillAdjustmentWriterConfiguration.setMaxBatchSize(jdbcWriterConfiguration.getMaxBatchSize());
    postBillAdjustmentWriterConfiguration.setIdempotencyRegistry(jdbcWriterConfiguration.getIdempotencyRegistry());
    postBillAdjustmentWriterConfiguration.setRepartitionTo(jdbcWriterConfiguration.getRepartitionTo());

    return new PostBillAdjustmentWriter(postBillAdjustmentWriterConfiguration);
  }
}
