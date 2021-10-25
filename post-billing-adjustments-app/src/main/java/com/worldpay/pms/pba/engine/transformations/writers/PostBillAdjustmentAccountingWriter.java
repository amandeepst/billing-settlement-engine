package com.worldpay.pms.pba.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.pba.engine.model.output.BillAdjustmentAccountingRow;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcBatchPartitionWriterFunction;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.sql2o.Query;

public class PostBillAdjustmentAccountingWriter extends JdbcWriter<BillAdjustmentAccountingRow> {

  public PostBillAdjustmentAccountingWriter(JdbcWriterConfiguration conf) {
    super(conf);
  }

  @Override
  protected JdbcBatchPartitionWriterFunction<BillAdjustmentAccountingRow> writer(BatchId batchId, Timestamp createdAt,
      JdbcWriterConfiguration conf) {
    return new Writer(batchId, createdAt, conf);
  }

  @Slf4j
  private static class Writer extends JdbcBatchPartitionWriterFunction.Simple<BillAdjustmentAccountingRow> {

    public Writer(BatchId batchCode, Timestamp batchStartedAt, JdbcWriterConfiguration conf) {
      super(batchCode, batchStartedAt, conf);
    }

    @Override
    protected String getStatement() {
      return resourceAsString("sql/outputs/insert-post-bill-adj-accounting.sql");
    }

    @Override
    protected void bindAndAdd(BillAdjustmentAccountingRow billAdjustment, Query query) {

      query
      .addParameter("post_bill_adj_id", billAdjustment.getId())
          .addParameter("party_id", billAdjustment.getPartyId())
          .addParameter("acct_id", billAdjustment.getAccountId())
          .addParameter("acct_type", billAdjustment.getAccountType())
          .addParameter("sub_acct_id", billAdjustment.getSubAccountId())
          .addParameter("sub_acct_type", billAdjustment.getSubAccountType())
          .addParameter("currency_cd", billAdjustment.getCurrencyCode())
          .addParameter("bill_amt", billAdjustment.getBillAmount())
          .addParameter("bill_id", billAdjustment.getBillId())
          .addParameter("amount", billAdjustment.getAdjustmentAmount())
          .addParameter("lcp", billAdjustment.getLegalCounterparty())
          .addParameter("business_unit", billAdjustment.getBusinessUnit())
          .addParameter("type", billAdjustment.getAdjustmentType())
          .addParameter("bill_dt", billAdjustment.getBillDate())
          .addToBatch();
    }

    @Override
    protected String name() {
      return "post-bill-adjustment-accounting";
    }
  }
}
