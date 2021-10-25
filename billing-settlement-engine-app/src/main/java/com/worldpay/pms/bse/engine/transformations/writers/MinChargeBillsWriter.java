package com.worldpay.pms.bse.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.SparkUtils.timed;

import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.MinimumChargeBillRow;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcBatchPartitionWriterFunction;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Timestamp;
import java.util.Iterator;
import lombok.extern.slf4j.Slf4j;
import org.sql2o.Connection;
import org.sql2o.Query;

public class MinChargeBillsWriter extends JdbcWriter<MinimumChargeBillRow> {

  public MinChargeBillsWriter(JdbcWriterConfiguration jdbcWriterConfiguration) {
    super(jdbcWriterConfiguration);
  }

  @Override
  protected JdbcBatchPartitionWriterFunction<MinimumChargeBillRow> writer(BatchId batchId, Timestamp timestamp,
      JdbcWriterConfiguration jdbcWriterConfiguration) {
    return new Writer(batchId, timestamp, jdbcWriterConfiguration);
  }

  @Slf4j
  private static class Writer extends JdbcBatchPartitionWriterFunction<MinimumChargeBillRow> {

    public Writer(BatchId batchCode, Timestamp batchStartedAt, JdbcWriterConfiguration conf) {
      super(batchCode, batchStartedAt, conf);
    }

    @Override
    protected String name() {
      return "min-charge-bill";
    }

    @Override
    protected void write(Connection conn, Iterator<MinimumChargeBillRow> partition) {
      try (Query insertPendingMinimumCharge = createQueryWithDefaults(conn,
          resourceAsString("sql/outputs/insert-minimum-charge-bill.sql"))) {

        bindAndExecuteStatements(partition, insertPendingMinimumCharge);
      }
    }

    private void bindAndExecuteStatements(Iterator<MinimumChargeBillRow> partition, Query insertPendingMinimumCharge) {
      partition.forEachRemaining(minChargeBills -> bindAndAdd(minChargeBills, insertPendingMinimumCharge));
      timed("insert-pending-minimum-charge", insertPendingMinimumCharge::executeBatch);
    }

    private void bindAndAdd(MinimumChargeBillRow row, Query stmt) {
      stmt.addParameter("bill_party_id", row.getBillPartyId())
          .addParameter("legal_counterparty", row.getLegalCounterparty())
          .addParameter("currency", row.getCurrency())
          .addParameter("logical_date", row.getLogicalDate())
          .addToBatch();
    }
  }
}
