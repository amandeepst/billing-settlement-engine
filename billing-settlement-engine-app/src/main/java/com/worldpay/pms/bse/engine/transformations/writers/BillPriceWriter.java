package com.worldpay.pms.bse.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.bse.engine.BillingConfiguration.BillPriceWriterConfiguration;
import com.worldpay.pms.bse.engine.transformations.model.billprice.BillPriceRow;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcBatchPartitionWriterFunction;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import java.sql.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.sql2o.Connection;
import org.sql2o.Query;
import scala.Tuple2;

public class BillPriceWriter extends JdbcWriter<Tuple2<String, BillPriceRow>> {

  private final int sourceKeyIncrementBy;
  private final String sourceKeySequenceName;
  private final String nonEventIdSequenceName;

  public BillPriceWriter(BillPriceWriterConfiguration conf) {
    super(conf);
    this.sourceKeyIncrementBy = conf.getSourceKeyIncrementBy();
    this.sourceKeySequenceName = conf.getSourceKeySequenceName();
    this.nonEventIdSequenceName = conf.getNonEventIdSequenceName();
  }

  @Override
  protected JdbcBatchPartitionWriterFunction<Tuple2<String, BillPriceRow>> writer(BatchId batchId, Timestamp startedAt,
      JdbcWriterConfiguration conf) {
    return new Writer(batchId, startedAt, conf, sourceKeySequenceName, sourceKeyIncrementBy, nonEventIdSequenceName);
  }

  @Slf4j
  private static class Writer extends JdbcBatchPartitionWriterFunction.Simple<Tuple2<String, BillPriceRow>> {

    private final String sequenceName;
    private final int sequenceIncrementBy;
    private final String nonEventIdSequenceName;

    // having the field at this level means the sequence will be shared across the sub-partitions (batches)
    // but in a thread safe manner since other partitions will have their own local `Writer` instances.
    private transient SequenceBasedIdGenerator incrementalIdProvider;
    private transient SequenceBasedIdGenerator nonEventIdProvider;

    Writer(BatchId batchCode, Timestamp batchStartedAt, JdbcWriterConfiguration conf, String sequenceName, int sourceKeyIncrementBy,
        String nonEventIdSequenceName) {
      super(batchCode, batchStartedAt, conf);

      this.sequenceName = sequenceName;
      this.sequenceIncrementBy = sourceKeyIncrementBy;
      this.nonEventIdSequenceName = nonEventIdSequenceName;
    }

    protected long getNextSourceKey(Connection conn) {
      if (incrementalIdProvider == null)
        incrementalIdProvider = new SequenceBasedIdGenerator(sequenceName, sequenceIncrementBy);

      return incrementalIdProvider.next(conn);
    }

    protected long getNextNonEventId(Connection conn){
      if (nonEventIdProvider == null)
        nonEventIdProvider = new SequenceBasedIdGenerator(nonEventIdSequenceName, sequenceIncrementBy);

      return nonEventIdProvider.next(conn);
    }

    @Override
    protected String getStatement() {
      return resourceAsString("sql/outputs/insert-bill-price.sql");
    }

    @Override
    protected void bindAndAdd(Tuple2<String, BillPriceRow> billPriceTuple, Query query) {
      BillPriceRow billPriceRow = billPriceTuple._2();

      query.addParameter("bill_price_id", billPriceTuple._1())
          .addParameter("party_id", billPriceRow.getPartyId())
          .addParameter("acct_type", billPriceRow.getAccountType())
          .addParameter("product_id", billPriceRow.getProductId())
          .addParameter("calc_ln_type", billPriceRow.getCalculationLineType())
          .addParameter("amount", billPriceRow.getAmount())
          .addParameter("currency_cd", billPriceRow.getCurrency())
          .addParameter("bill_reference", billPriceRow.getBillReference())
          .addParameter("invoiceable_flg", billPriceRow.getInvoiceableFlag())
          .addParameter("source_type", billPriceRow.getSourceType())
          .addParameter("source_id", billPriceRow.getSourceId())
          .addParameter("bill_ln_dtl_id", billPriceRow.getBillLineDetailId())
          .addParameter("misc_bill_item_id", billPriceRow.getBillableItemId())
          .addParameter("bill_id", billPriceRow.getBillId())
          .addParameter("accrued_dt", billPriceRow.getAccruedDate())
          .addParameter("source_key", getNextSourceKey(query.getConnection()))
          .addParameter("granularity", billPriceRow.getGranularity())
          .addParameter("non_event_id", getNextNonEventId(query.getConnection()))
          .addToBatch();
    }

    @Override
    protected String name() {
      return "bill-price";
    }
  }

}
