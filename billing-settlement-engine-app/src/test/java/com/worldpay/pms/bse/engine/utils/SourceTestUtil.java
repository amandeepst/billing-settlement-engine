package com.worldpay.pms.bse.engine.utils;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.bse.domain.exception.BillingException;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.batch.Batch.BatchStep;
import com.worldpay.pms.spark.core.batch.Batch.Watermark;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import java.sql.Date;
import java.sql.Timestamp;
import lombok.experimental.UtilityClass;
import org.sql2o.Connection;
import org.sql2o.Sql2oException;
import org.sql2o.Sql2oQuery;

@UtilityClass
public class SourceTestUtil {


  public static Batch insertBatchHistoryAndOnBatchCompleted(SqlDb sqlDb, String batchCode, String state, Timestamp lowWatermark,
      Timestamp highWatermark, Timestamp createdAt, Timestamp ilmDt, Date logicalDate, String datasetId) {
    Batch batch = new Batch(new BatchId(batchCode, new Watermark(lowWatermark.toLocalDateTime(), highWatermark.toLocalDateTime()), 1),
        BatchStep.valueOf(state), createdAt.toLocalDateTime(), null, null);

    sqlDb.exec("insert_batch_history_and_on_batch_completed", conn -> {
      if ("COMPLETED".equals(state)) {
        onBatchCompleted(conn, batch, ilmDt, logicalDate, datasetId);
      }
      return insertBatchHistory(conn, batch);
    });

    return batch;
  }

  public static void deleteBatchHistoryAndOnBatchCompleted(SqlDb db, String batchCode) {
    db.execQuery("delete-batch-history", resourceAsString("sql/batch_history/delete_batch_history_by_batch_code.sql"),
        (query) -> query.addParameter("batch_code", batchCode).executeUpdate());
    db.execQuery("delete-batch-history", resourceAsString("sql/outputs_registry/delete_outputs_registry_by_batch_code.sql"),
        (query) -> query.addParameter("batch_code", batchCode).executeUpdate());
  }

  private void onBatchCompleted(Connection conn, Batch batch, Timestamp ilmDt, Date logicalDate, String datasetId) {
    Sql2oQuery query = new Sql2oQuery(conn, resourceAsString("sql/outputs_registry/insert_outputs_registry.sql"));

    try {
      query.addParameter("batch_code", batch.id.code)
          .addParameter("batch_attempt", batch.id.attempt)
          .addParameter("ilm_dt", ilmDt)
          .addParameter("logical_date", logicalDate)
          .addParameter("dataset_id", datasetId)
          .executeUpdate();

    } catch (Sql2oException e) {
      throw new BillingException(e, "Unhandled exception when executing query:insert on completed batch");
    }
  }

  private Batch insertBatchHistory(Connection conn, Batch batch) {
    Sql2oQuery query = new Sql2oQuery(conn, resourceAsString("sql/batch_history/insert_batch_history.sql"));

    try {
      query.addParameter("batch_code", batch.id.code)
          .addParameter("attempt", batch.id.attempt)
          .addParameter("state", batch.step.name())
          .addParameter("watermark_low", Timestamp.valueOf(batch.id.watermark.low))
          .addParameter("watermark_high", Timestamp.valueOf(batch.id.watermark.high))
          .addParameter("comments", "")
          .addParameter("metadata", "")
          .addParameter("created_at", Date.valueOf(batch.createdAt.toLocalDate()))
          .executeUpdate();
    } catch (Sql2oException e) {
      throw new BillingException(e, "Unhandled exception when executing query:insert batch history");
    }

    return batch;
  }
}
