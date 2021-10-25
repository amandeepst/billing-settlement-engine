package com.worldpay.pms.pba.engine.data;

import static com.worldpay.pms.pba.engine.common.Utils.getCurrentDateTimeWithSecondRoundedUp;
import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.SparkUtils.timed;

import com.google.common.io.Resources;
import com.worldpay.pms.pba.engine.PostBillingBatchRunResult;
import com.worldpay.pms.pba.engine.PostBillingConfiguration.PublisherConfiguration;
import com.worldpay.pms.spark.core.PMSException;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.batch.Batch.BatchStep;
import com.worldpay.pms.spark.core.batch.BatchMetadata;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.repositories.JdbcBatchHistoryRepository;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.sql2o.Connection;
import org.sql2o.Query;
import org.sql2o.Sql2oException;
import org.sql2o.Sql2oQuery;

@Slf4j
public class PostBillingBatchHistoryRepository extends JdbcBatchHistoryRepository<PostBillingBatchRunResult> {

  private static final String POST_BILLING_BATCH_HISTORY_TABLE = "batch_history_post_billing";
  private static final String BATCH_CODE_COL_NAME = "batch_code";
  private static final String BATCH_ATTEMPT_COL_NAME = "batch_attempt";
  private static final List<String> DATASET_IDS = Arrays.asList("POST_BILL_ADJ", "POST_BILL_ADJ_ACCOUNTING");

  private final Date logicalDate;
  private final PublisherConfiguration publisherConfig;

  public PostBillingBatchHistoryRepository(JdbcConfiguration conf, LocalDate logicalDate, PublisherConfiguration publisherConfig) {
    super(POST_BILLING_BATCH_HISTORY_TABLE, PostBillingBatchRunResult.class, conf);
    this.logicalDate = Date.valueOf(logicalDate);
    this.publisherConfig = publisherConfig;
  }

  @Override
  public Batch addBatch(BatchId id, BatchStep step, Optional<String> comment, Optional<BatchMetadata> metadata) {
    return super.addBatch(id, step, comment.orElse(null), metadata.orElse(null), getCurrentDateTimeWithSecondRoundedUp());
  }

  @Override
  public void onBatchCompleted(Connection conn, Batch batch) {
    String stmt = resourceAsString(
        Resources.getResource(PostBillingBatchHistoryRepository.class, "/sql/output-registry/insert-outputs-registry.sql"));
    Sql2oQuery query = new Sql2oQuery(conn, stmt);

    try {
      query.addParameter(BATCH_CODE_COL_NAME, batch.id.code)
          .addParameter(BATCH_ATTEMPT_COL_NAME, batch.id.attempt)
          .addParameter("ilm_dt", Timestamp.valueOf(batch.createdAt))
          .addParameter("logical_date", logicalDate);

      DATASET_IDS.forEach(datasetId -> {
        query.addParameter("dataset_id", datasetId);
        query.executeUpdate();
      });
    } catch (Sql2oException e) {
      throw new PMSException(e, "Unhandled exception when executing query:\n%s", stmt);
    }

    publishAll(publisherConfig, conn, batch);
  }

  public long generateRunId(BatchId batchId) {
    return db.exec("generate-run-id",
        conn -> conn.createQuery(resourceAsString("sql/batch/get-batch-seed.sql"))
            .addParameter(BATCH_CODE_COL_NAME, batchId.code)
            .addParameter(BATCH_ATTEMPT_COL_NAME, batchId.attempt)
            .executeScalar(long.class));
  }


  private static void publishAll(PublisherConfiguration conf, Connection conn, Batch batch) {
    if (conf.isPublishBillPayment()) {
      timed("publish-outputs", () -> {
        publish("bill-payment-detail", conf.getBillPaymentDetail(), conn, batch);
        publish("bill-payment-detail-snapshot", conf.getBillPaymentDetailSnapshot(), conn, batch);
      });
    }
  }

  private static void publish(String name, String hints, Connection conn, Batch batch) {
    String sql = interpolateHints(resourceAsString(String.format("sql/publish/publish-%s.sql", name)), hints);

    log.info("Executing `{}`...\n{}", name, sql);
    timed(String.format("publish-%s", name), () -> {
      try (Query stmt = conn.createQuery(sql)) {
        stmt.addParameter(BATCH_CODE_COL_NAME, batch.id.code)
            .addParameter(BATCH_ATTEMPT_COL_NAME, batch.id.attempt)
            .addParameter("ilm_dt", Timestamp.valueOf(batch.createdAt))
            .executeUpdate();
      }
    });
  }

  static String interpolateHints(String sql, String hints) {
    String[] h = hints == null ? new String[0] : hints.split(":");
    return sql.replace(":insert-hints", h.length > 0 ? h[0] : "")
        .replace(":select-hints", h.length > 1 ? h[1] : "");
  }
}
