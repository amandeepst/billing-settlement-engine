package com.worldpay.pms.bse.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.bse.engine.transformations.model.failedbillableitem.FailedBillableItem;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcBatchPartitionWriterFunction;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.sql2o.Query;

public class FixedFailedBillableItemWriter extends JdbcWriter<FailedBillableItem> {

  private static final String BILL_ITEM_ID_COL_NAME = "bill_item_id";
  private static final String FIRST_FAILURE_ON_COL_NAME = "first_failure_on";
  private static final String RETRY_COUNT_COL_NAME = "retry_count";
  private static final String BILLABLE_ITEM_ILM_DT_COL_NAME = "billable_item_ilm_dt";
  private static final String ROOT = "sql/outputs/";

  public FixedFailedBillableItemWriter(JdbcWriterConfiguration conf) {
    super(conf);
  }

  @Override
  protected JdbcBatchPartitionWriterFunction<FailedBillableItem> writer(BatchId batchId, Timestamp timestamp,
                                                                        JdbcWriterConfiguration conf) {
    return new FixedFailedBillableItemWriter.Writer(batchId, timestamp, conf);
  }

  @Slf4j
  private static class Writer extends JdbcBatchPartitionWriterFunction.Simple<FailedBillableItem> {

    public Writer(BatchId batchCode, Timestamp batchStartedAt, JdbcWriterConfiguration conf) {
      super(batchCode, batchStartedAt, conf);
    }

    @Override
    protected String getStatement() {
      return resourceAsString(ROOT + "insert-fixed-bill-item-error.sql");
    }

    @Override
    protected void bindAndAdd(FailedBillableItem failedBillableItem, Query stmt) {
      stmt.addParameter(BILL_ITEM_ID_COL_NAME, failedBillableItem.getBillItemId())
          .addParameter(FIRST_FAILURE_ON_COL_NAME, failedBillableItem.getFirstFailureOn())
          .addParameter(RETRY_COUNT_COL_NAME, failedBillableItem.getRetryCount())
          .addParameter(BILLABLE_ITEM_ILM_DT_COL_NAME, failedBillableItem.getBillableItemIlmDt())
          .addToBatch();
    }

    @Override
    protected String name() {
      return "fixed-billable-items";
    }
  }
}
