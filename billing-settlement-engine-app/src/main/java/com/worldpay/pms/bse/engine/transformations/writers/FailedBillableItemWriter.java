package com.worldpay.pms.bse.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.bse.engine.transformations.model.failedbillableitem.FailedBillableItem;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcErrorWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.sql2o.Connection;
import org.sql2o.Query;

public class FailedBillableItemWriter extends JdbcErrorWriter<FailedBillableItem> {

  private static final String BILL_ITEM_ID_COL_NAME = "bill_item_id";
  private static final String FIRST_FAILURE_ON_COL_NAME = "first_failure_on";
  private static final String ERROR_DATE_COL_NAME = "billable_item_ilm_dt";
  private static final String RETRY_COUNT_COL_NAME = "retry_count";
  private static final String CODE_COL_NAME = "code";
  private static final String REASON_COL_NAME = "reason";
  private static final String STATUS_COL_NAME = "status";
  private static final String STACK_TRACE_COL_NAME = "stack_trace";
  private static final String ROOT = "sql/outputs/";

  public FailedBillableItemWriter(JdbcWriterConfiguration conf, SparkSession spark) {
    super(conf, spark);
  }

  @Override
  protected ErrorPartitionWriter<FailedBillableItem> createWriter(BatchId batchId, Timestamp timestamp,
                                                                  JdbcWriterConfiguration jdbcWriterConfiguration,
                                                                  LongAccumulator firstFailures, LongAccumulator ignored) {
    return new Writer(batchId,timestamp,jdbcWriterConfiguration, firstFailures, ignored);
  }

  @Slf4j
  protected static class Writer extends JdbcErrorWriter.ErrorPartitionWriter<FailedBillableItem> {

    public Writer(BatchId batchCode, Timestamp batchStartedAt, JdbcWriterConfiguration conf,
                  LongAccumulator firstFailures, LongAccumulator ignored) {
      super(batchCode, batchStartedAt, conf, firstFailures, ignored);
    }

    @Override
    protected String name() {
      return "failed-billable-items";
    }

    @Override
    protected void bindAndAdd(FailedBillableItem failedBillableItem, Timestamp timestamp, Query stmt) {
      stmt.addParameter(BILL_ITEM_ID_COL_NAME, failedBillableItem.getBillItemId())
          .addParameter(FIRST_FAILURE_ON_COL_NAME, failedBillableItem.getFirstFailureOn())
          .addParameter(RETRY_COUNT_COL_NAME, failedBillableItem.getRetryCount())
          .addParameter(CODE_COL_NAME, failedBillableItem.getCode())
          .addParameter(STATUS_COL_NAME, failedBillableItem.getStatus())
          .addParameter(REASON_COL_NAME, failedBillableItem.getReason())
          .addParameter(STACK_TRACE_COL_NAME, failedBillableItem.getStackTrace())
          .addParameter(ERROR_DATE_COL_NAME, failedBillableItem.getBillableItemIlmDt())
          .addToBatch();
    }

    @Override
    protected Query createStatement(Connection connection) {
      return createQueryWithDefaults(connection, resourceAsString(ROOT + "insert-bill-item-error.sql"));
    }
  }
}
