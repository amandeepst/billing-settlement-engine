package com.worldpay.pms.bse.engine.transformations.view;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.bse.domain.exception.BillingException;
import com.worldpay.pms.bse.engine.data.BillingBatchHistoryRepository;
import com.worldpay.pms.bse.engine.utils.WithDatabase;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.batch.Batch.BatchStep;
import com.worldpay.pms.spark.core.batch.Batch.Watermark;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import org.sql2o.Connection;
import org.sql2o.Sql2oException;
import org.sql2o.Sql2oQuery;

public abstract class ViewBaseTest implements WithDatabase {

  protected static final Timestamp GENERIC_TIMESTAMP = Timestamp.valueOf(LocalDateTime.parse("2020-04-28T16:00:00"));
  protected static final Date GENERIC_DATE = Date.valueOf("2020-04-28");
  protected static final Timestamp TS_20200429T130000 = Timestamp.valueOf(LocalDateTime.parse("2020-04-29T13:00:00"));

  protected static final String STATE_COMPLETED = "COMPLETED";
  protected static final String STATE_FAILED = "FAILED";

  protected static final String QUERY_INSERT_BATCH_HISTORY = resourceAsString("sql/batch_history/insert_batch_history.sql");
  protected static final String QUERY_INSERT_OUTPUTS_REGISTRY = resourceAsString("sql/outputs_registry/insert_outputs_registry.sql");

  protected SqlDb sqlDb;

  @Override
  public void bindBillingJdbcConfiguration(JdbcConfiguration conf) {
    sqlDb = SqlDb.simple(conf);
  }

  void insertBatchHistoryAndOnBatchCompleted(String batchCode, int attempt, String state) {
    Batch batch = new Batch(
        new BatchId(batchCode, new Watermark(TS_20200429T130000.toLocalDateTime(), TS_20200429T130000.toLocalDateTime()), attempt),
        BatchStep.valueOf(state), LocalDateTime.now(), null, null);

    sqlDb.exec("insert_batch_history_and_on_batch_completed", conn -> {
      if (STATE_COMPLETED.equals(state)) {
        onBatchCompleted(conn, batch);
      }
      return insertBatchHistory(conn, batch);
    });
  }

  private void onBatchCompleted(Connection conn, Batch batch) {
    Sql2oQuery query = new Sql2oQuery(conn, QUERY_INSERT_OUTPUTS_REGISTRY);

    try {
      query.addParameter("batch_code", batch.id.code)
          .addParameter("batch_attempt", batch.id.attempt)
          .addParameter("ilm_dt", GENERIC_TIMESTAMP)
          .addParameter("logical_date", GENERIC_DATE);

      BillingBatchHistoryRepository.getDatasetIds().forEach(datasetId -> {
        query.addParameter("dataset_id", datasetId);
        query.executeUpdate();
      });

    } catch (Sql2oException e) {
      throw new BillingException(e, "Unhandled exception when executing query:insert on completed batch");
    }
  }

  private Batch insertBatchHistory(Connection conn, Batch batch) {
    Sql2oQuery query = new Sql2oQuery(conn, QUERY_INSERT_BATCH_HISTORY);

    try {
      query.addParameter("batch_code", batch.id.code)
          .addParameter("attempt", batch.id.attempt)
          .addParameter("state", batch.step.name())
          .addParameter("watermark_low", TS_20200429T130000)
          .addParameter("watermark_high", TS_20200429T130000)
          .addParameter("comments", "")
          .addParameter("metadata", "")
          .addParameter("created_at", TS_20200429T130000);
      query.executeUpdate();
    } catch (Sql2oException e) {
      throw new BillingException(e, "Unhandled exception when executing query:insert batch history");
    }

    return batch;
  }


}