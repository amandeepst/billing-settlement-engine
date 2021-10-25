package com.worldpay.pms.bse.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.SparkUtils.timed;

import com.worldpay.pms.bse.engine.transformations.model.billerror.BillError;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcBatchPartitionWriterFunction;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Timestamp;
import java.util.Iterator;
import lombok.extern.slf4j.Slf4j;
import org.sql2o.Connection;
import org.sql2o.Query;

public class FixedBillErrorWriter extends JdbcWriter<BillError> {

  private static final String BILL_ID_COL_NAME = "bill_id";

  public FixedBillErrorWriter(JdbcWriterConfiguration conf) {
    super(conf);
  }

  @Override
  protected JdbcBatchPartitionWriterFunction<BillError> writer(BatchId batchId, Timestamp startedAt,
      JdbcWriterConfiguration conf) {
    return new Writer(batchId, startedAt, conf);
  }

  @Slf4j
  private static class Writer extends JdbcBatchPartitionWriterFunction<BillError> {


    public Writer(BatchId batchCode, Timestamp batchStartedAt, JdbcWriterConfiguration conf) {
      super(batchCode, batchStartedAt, conf);
    }

    @Override
    protected String name() {
      return "fixed-bill-error";
    }

    @Override
    protected void write(Connection conn, Iterator<BillError> partition) {
      try (Query insertFixedBillError = createQueryWithDefaults(conn, resourceAsString("sql/outputs/insert-fixed-bill-error.sql"))) {

        bindAndExecuteStatements(
            partition,
            insertFixedBillError
        );
      }
    }

    private void bindAndExecuteStatements(Iterator<BillError> partition, Query insertFixedBillError) {
      partition.forEachRemaining(fixedBillError -> bindAndAdd(fixedBillError, insertFixedBillError));
      timed("insert-fixed-bill-error", insertFixedBillError::executeBatch);
    }

    private void bindAndAdd(BillError fixedBillError, Query stmt) {
      stmt.addParameter(BILL_ID_COL_NAME, fixedBillError.getBillId())
          .addParameter("bill_dt", fixedBillError.getBillDate())
          .addParameter("first_failure_on", fixedBillError.getFirstFailureOn())
          .addParameter("retry_count", fixedBillError.getRetryCount())
          .addToBatch();
    }
  }
}
