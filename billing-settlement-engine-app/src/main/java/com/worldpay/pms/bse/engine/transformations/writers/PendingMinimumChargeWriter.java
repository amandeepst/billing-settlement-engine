package com.worldpay.pms.bse.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.SparkUtils.timed;

import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.PendingMinChargeRow;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcBatchPartitionWriterFunction;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Timestamp;
import java.util.Iterator;
import lombok.extern.slf4j.Slf4j;
import org.sql2o.Connection;
import org.sql2o.Query;

public class PendingMinimumChargeWriter extends JdbcWriter<PendingMinChargeRow> {

  public PendingMinimumChargeWriter(JdbcWriterConfiguration conf) {
    super(conf);
  }

  @Override
  protected JdbcBatchPartitionWriterFunction<PendingMinChargeRow> writer(BatchId batchId, Timestamp timestamp,
      JdbcWriterConfiguration conf) {
    return new Writer(batchId, timestamp, conf);
  }

  @Slf4j
  private static class Writer extends JdbcBatchPartitionWriterFunction<PendingMinChargeRow> {

    public Writer(BatchId batchCode, Timestamp batchStartedAt, JdbcWriterConfiguration conf) {
      super(batchCode, batchStartedAt, conf);
    }

    @Override
    protected String name() {
      return "pending-min-charge";
    }

    @Override
    protected void write(Connection conn, Iterator<PendingMinChargeRow> partition) {
      try (Query insertPendingMinimumCharge = createQueryWithDefaults(conn,
          resourceAsString("sql/outputs/insert-pending-minimum-charge.sql"))) {

        bindAndExecuteStatements(partition, insertPendingMinimumCharge);
      }
    }

    private void bindAndExecuteStatements(Iterator<PendingMinChargeRow> partition, Query insertPendingMinimumCharge) {
      partition.forEachRemaining(localPendingMinimumCharge -> bindAndAdd(localPendingMinimumCharge, insertPendingMinimumCharge));

      timed("insert-pending-minimum-charge", insertPendingMinimumCharge::executeBatch);
    }

    private void bindAndAdd(PendingMinChargeRow row, Query stmt) {
      stmt.addParameter("bill_party_id", row.getBillPartyId())
          .addParameter("legal_counterparty", row.getLegalCounterparty())
          .addParameter("txn_party_id", row.getTxnPartyId())
          .addParameter("min_chg_start_dt", row.getMinChargeStartDate())
          .addParameter("min_chg_end_dt", row.getMinChargeEndDate())
          .addParameter("min_chg_type", row.getMinChargeType())
          .addParameter("applicable_charges", row.getApplicableCharges())
          .addParameter("currency", row.getCurrency())
          .addParameter("bill_dt", row.getBillDate())
          .addToBatch();
    }
  }
}