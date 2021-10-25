package com.worldpay.pms.bse.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.SparkUtils.timed;

import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetailRow;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcBatchPartitionWriterFunction;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Timestamp;
import java.util.Iterator;
import lombok.extern.slf4j.Slf4j;
import org.sql2o.Connection;
import org.sql2o.Query;

public class BillLineDetailWriter extends JdbcWriter<BillLineDetailRow> {

  private static final String PENDING_BILL_ID_COL_NAME = "bill_id";
  private static final String PENDING_BILL_LINE_ID_COL_NAME = "bill_ln_id";

  public BillLineDetailWriter(JdbcWriterConfiguration conf) {
    super(conf);
  }

  @Override
  protected JdbcBatchPartitionWriterFunction<BillLineDetailRow> writer(BatchId batchId, Timestamp timestamp,
      JdbcWriterConfiguration conf) {

    return new Writer(batchId, timestamp, conf);
  }

  @Slf4j
  private static class Writer extends JdbcBatchPartitionWriterFunction<BillLineDetailRow> {

    public Writer(BatchId batchCode, Timestamp batchStartedAt, JdbcWriterConfiguration conf) {
      super(batchCode, batchStartedAt, conf);
    }

    @Override
    protected String name() {
      return "bill-line-detail";
    }

    @Override
    protected void write(Connection conn, Iterator<BillLineDetailRow> partition) {
      try (Query insertLineDetail = createQueryWithDefaults(conn, resourceAsString("sql/outputs/insert-bill-line-detail.sql"))) {
        bindAndExecuteStatements(partition, insertLineDetail);
      }
    }

    private void bindAndExecuteStatements(Iterator<BillLineDetailRow> partition, Query insertLineDetail) {
      partition.forEachRemaining(billLineDetail -> bindAndAdd(billLineDetail, insertLineDetail));
      timed("insert-bill-line-detail", insertLineDetail::executeBatch);
    }

    private void bindAndAdd(BillLineDetailRow billLineDetail, Query stmt) {
      stmt.addParameter(PENDING_BILL_ID_COL_NAME, billLineDetail.getBillId())
          .addParameter(PENDING_BILL_LINE_ID_COL_NAME, billLineDetail.getBillLineId())
          .addParameter("bill_item_id", billLineDetail.getBillItemId())
          .addParameter("bill_item_hash", billLineDetail.getBillItemHash())
          .addToBatch();
    }
  }
}
