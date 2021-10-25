package com.worldpay.pms.bse.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.bse.engine.transformations.model.billaccounting.BillAccountingRow;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcBatchPartitionWriterFunction;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.sql2o.Query;

public class BillAccountingWriter extends JdbcWriter<BillAccountingRow> {

  public BillAccountingWriter(JdbcWriterConfiguration conf) {
    super(conf);
  }

  @Override
  protected JdbcBatchPartitionWriterFunction<BillAccountingRow> writer(BatchId batchId, Timestamp startedAt, JdbcWriterConfiguration conf) {
    return new Writer(batchId, startedAt, conf);
  }

  @Slf4j
  private static class Writer extends JdbcBatchPartitionWriterFunction.Simple<BillAccountingRow> {

    public Writer(BatchId batchCode, Timestamp batchStartedAt, JdbcWriterConfiguration conf) {
      super(batchCode, batchStartedAt, conf);
    }

    @Override
    protected String getStatement() {
      return resourceAsString("sql/outputs/insert-bill-accounting.sql");
    }

    @Override
    protected void bindAndAdd(BillAccountingRow row, Query stmt) {
      stmt.addParameter("party_id", row.getPartyId())
          .addParameter("acct_id", row.getAccountId())
          .addParameter("acct_type", row.getAccountType())
          .addParameter("bill_sub_acct_id", row.getBillSubAccountId())
          .addParameter("sub_acct_type", row.getSubAccountType())
          .addParameter("currency_cd", row.getCurrency())
          .addParameter("bill_amt", row.getBillAmount())
          .addParameter("bill_id", row.getBillId())
          .addParameter("bill_ln_id", row.getBillLineId())
          .addParameter("total_line_amount", row.getTotalLineAmount())
          .addParameter("lcp", row.getLegalCounterparty())
          .addParameter("business_unit", row.getBusinessUnit())
          .addParameter("class", row.getBillClass())
          .addParameter("calc_ln_id", row.getCalculationLineId())
          .addParameter("type", row.getType())
          .addParameter("calc_ln_type", row.getCalculationLineType())
          .addParameter("calc_line_amount", row.getCalculationLineAmount())
          .addParameter("bill_dt", row.getBillDate())
          .addToBatch();
    }

    @Override
    protected String name() {
      return "bill-accounting";
    }


  }
}
