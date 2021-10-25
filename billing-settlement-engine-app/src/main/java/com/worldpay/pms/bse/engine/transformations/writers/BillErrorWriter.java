package com.worldpay.pms.bse.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.SparkUtils.timed;

import com.worldpay.pms.bse.engine.transformations.model.billerror.BillError;
import com.worldpay.pms.bse.engine.transformations.model.billerror.BillErrorDetail;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcErrorWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import io.vavr.collection.Array;
import java.sql.Timestamp;
import java.util.Iterator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.sql2o.Connection;
import org.sql2o.Query;

public class BillErrorWriter extends JdbcErrorWriter<BillError> {

  private static final String BILL_ID_COL_NAME = "bill_id";


  public BillErrorWriter(JdbcWriterConfiguration conf, SparkSession spark) {
    super(conf, spark);
  }

  @Override
  protected ErrorPartitionWriter<BillError> createWriter(BatchId batchId, Timestamp startedAt, JdbcWriterConfiguration conf,
      LongAccumulator firstFailureCounter, LongAccumulator ignoredCounter) {
    return new Writer(batchId, startedAt, conf, firstFailureCounter, ignoredCounter);
  }


  @Slf4j
  private static class Writer extends JdbcErrorWriter.ErrorPartitionWriter<BillError> {

    Writer(BatchId batchCode, Timestamp batchStartedAt, JdbcWriterConfiguration conf, LongAccumulator firstFailures,
        LongAccumulator ignored) {
      super(batchCode, batchStartedAt, conf, firstFailures, ignored);
    }

    @Override
    protected String name() {
      return "bill-error";
    }

    @Override
    protected void write(Connection conn, Iterator<BillError> partition) {
      try (Query insertBillError = createQueryWithDefaults(conn, resourceAsString("sql/outputs/insert-bill-error.sql"));
          Query insertBillErrorDetail = createQueryWithDefaults(conn, resourceAsString("sql/outputs/insert-bill-error-detail.sql"))) {

        partition.forEachRemaining(billError -> {
          if (billError.isIgnored()) {
            ignoredCounter.add(1);
          } else if (billError.isFirstFailure()) {
            firstFailureCounter.add(1);
          }

          bindAndAdd(billError, this.startedAt, insertBillError);

          BillErrorDetail[] details = billError.getBillErrorDetails();
          if (details != null) {
            Array.of(details)
                .forEach(billErrorDetail -> bindAndAdd(billError.getBillId(), billErrorDetail, insertBillErrorDetail));
          }
        });

        timed("insert-bill-error", insertBillError::executeBatch);
        timed("insert-bill-error-detail", insertBillErrorDetail::executeBatch);
      }
    }

    @Override
    protected void bindAndAdd(BillError billError, Timestamp startedAt, Query stmt) {
      stmt.addParameter(BILL_ID_COL_NAME, billError.getBillId())
          .addParameter("bill_dt", billError.getBillDate())
          .addParameter("first_failure_on", billError.getFirstFailureOn())
          .addParameter("retry_count", billError.getRetryCount())
          .addToBatch();

    }

    private void bindAndAdd(String billId, BillErrorDetail billErrorDetail, Query stmt) {
      stmt.addParameter(BILL_ID_COL_NAME, billId)
          .addParameter("bill_ln_id", billErrorDetail.getBillLineId())
          .addParameter("code", billErrorDetail.getCode())
          .addParameter("reason", billErrorDetail.getMessage())
          .addParameter("stack_trace", billErrorDetail.getStackTrace())
          .addToBatch();
    }

    protected Query createStatement(Connection conn) {
      return null;
    }
  }
}