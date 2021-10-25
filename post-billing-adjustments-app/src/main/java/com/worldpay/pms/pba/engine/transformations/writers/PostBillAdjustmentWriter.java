package com.worldpay.pms.pba.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.pba.domain.model.BillAdjustment;
import com.worldpay.pms.pba.engine.PostBillingConfiguration.PostBillAdjustmentWriterConfiguration;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcBatchPartitionWriterFunction;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.sql2o.Connection;
import org.sql2o.Query;
import scala.Tuple2;

public class PostBillAdjustmentWriter extends JdbcWriter<Tuple2<String, BillAdjustment>> {

  private final int sourceKeyIncrementBy;
  private final String sourceKeySequenceName;

  public PostBillAdjustmentWriter(PostBillAdjustmentWriterConfiguration conf) {
    super(conf);
    this.sourceKeyIncrementBy = conf.getSourceKeyIncrementBy();
    this.sourceKeySequenceName = conf.getSourceKeySequenceName();
  }

  @Override
  protected JdbcBatchPartitionWriterFunction<Tuple2<String, BillAdjustment>> writer(BatchId batchId, Timestamp createdAt,
      JdbcWriterConfiguration conf) {
    return new Writer(batchId, createdAt, conf, sourceKeySequenceName, sourceKeyIncrementBy);
  }

  @Slf4j
  private static class Writer extends JdbcBatchPartitionWriterFunction.Simple<Tuple2<String, BillAdjustment>> {

    private final String sequenceName;
    private final int sequenceIncrementBy;

    // having the field at this level means the sequence will be shared across the sub-partitions (batches)
    // but in a thread safe manner since other partitions will have their own local `Writer` instances.
    private transient SequenceBasedIdGenerator incrementalIdProvider;

    public Writer(BatchId batchCode, Timestamp batchStartedAt, JdbcWriterConfiguration conf,
        String sequenceName, int sourceKeyIncrementBy) {
      super(batchCode, batchStartedAt, conf);

      this.sequenceName = sequenceName;
      this.sequenceIncrementBy = sourceKeyIncrementBy;
    }

    protected long getNextSourceKey(Connection conn) {
      if (incrementalIdProvider == null)
        incrementalIdProvider = new SequenceBasedIdGenerator(sequenceName, sequenceIncrementBy);

      return incrementalIdProvider.next(conn);
    }

    @Override
    protected String getStatement() {
      return resourceAsString("sql/outputs/insert-post-bill-adj.sql");
    }

    @Override
    protected void bindAndAdd(Tuple2<String, BillAdjustment> billAdjustmentWithId, Query query) {

      String id = billAdjustmentWithId._1;
      BillAdjustment billAdjustment = billAdjustmentWithId._2;

      query.addParameter("post_bill_adj_id", id)
          .addParameter("post_bill_adj_type", billAdjustment.getAdjustment().getType())
          .addParameter("post_bill_adj_descr", billAdjustment.getAdjustment().getDescription())
          .addParameter("bill_id", billAdjustment.getBill().getBillId())
          .addParameter("amount", billAdjustment.getAdjustment().getAmount())
          .addParameter("currency_cd", billAdjustment.getBill().getCurrencyCode())
          .addParameter("bill_sub_acct_id", billAdjustment.getBill().getBillSubAccountId())
          .addParameter("rel_sub_acct_id", billAdjustment.getAdjustment().getSubAccountId())
          .addParameter("party_id", billAdjustment.getBill().getPartyId())
          .addParameter("bill_ref", billAdjustment.getBill().getBillReference())
          .addParameter("granularity", billAdjustment.getBill().getGranularity())
          .addParameter("acct_type", billAdjustment.getBill().getAccountType())
          .addParameter("bill_dt", billAdjustment.getBill().getBillDate())
          .addParameter("source_key", getNextSourceKey(query.getConnection()))
          .addToBatch();
    }

    @Override
    protected String name() {
      return "post-bill-adjustment";
    }
  }
}
