package com.worldpay.pms.bse.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.bse.engine.transformations.model.billrelationship.BillRelationshipRow;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcBatchPartitionWriterFunction;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.sql2o.Query;

public class BillRelationshipWriter extends JdbcWriter<BillRelationshipRow> {

  public BillRelationshipWriter(JdbcWriterConfiguration conf) {
    super(conf);
  }

  @Override
  protected JdbcBatchPartitionWriterFunction<BillRelationshipRow> writer(BatchId batchId, Timestamp startedAt,
      JdbcWriterConfiguration conf) {
    return new Writer(batchId, startedAt, conf);
  }

  @Slf4j
  private static class Writer extends JdbcBatchPartitionWriterFunction.Simple<BillRelationshipRow> {

    Writer(BatchId batchCode, Timestamp batchStartedAt, JdbcWriterConfiguration conf) {
      super(batchCode, batchStartedAt, conf);
    }

    @Override
    protected String getStatement() {
      return resourceAsString("sql/outputs/insert-bill-relationship.sql");
    }

    @Override
    protected void bindAndAdd(BillRelationshipRow billRelationshipRow, Query query) {
      query
          .addParameter("parent_bill_id", billRelationshipRow.getParentBillId())
          .addParameter("child_bill_id", billRelationshipRow.getChildBillId())
          .addParameter("relationship_type", billRelationshipRow.getRelationshipTypeId())
          .addParameter("event_id", billRelationshipRow.getEventId())
          .addParameter("paid_invoice", billRelationshipRow.getPaidInvoice())
          .addParameter("reuse_due_date", billRelationshipRow.getReuseDueDate())
          .addParameter("type_cd", billRelationshipRow.getType())
          .addParameter("reason_cd", billRelationshipRow.getReasonCd())
          .addToBatch();
    }

    @Override
    protected String name() {
      return "bill-relationship";
    }

  }

}
