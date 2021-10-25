package com.worldpay.pms.bse.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.SparkUtils.timed;

import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTaxDetail;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcBatchPartitionWriterFunction;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import io.vavr.collection.Array;
import java.sql.Timestamp;
import java.util.Iterator;
import lombok.extern.slf4j.Slf4j;
import org.sql2o.Connection;
import org.sql2o.Query;

public class BillTaxWriter extends JdbcWriter<BillTax> {

  public static final String BILL_ID_COLUMN_NAME = "bill_id";
  public static final String BILL_TAX_ID_COLUMN_NAME = "bill_tax_id";
  public static final String REV_CHG_FLG_COLUMN_NAME = "rev_chg_flg";

  public BillTaxWriter(JdbcWriterConfiguration conf) {
    super(conf);
  }

  @Override
  protected JdbcBatchPartitionWriterFunction<BillTax> writer(BatchId batchId, Timestamp startedAt, JdbcWriterConfiguration conf) {
    return new Writer(batchId, startedAt, conf);
  }

  @Slf4j
  private static class Writer extends JdbcBatchPartitionWriterFunction<BillTax> {


    public Writer(BatchId batchCode, Timestamp batchStartedAt, JdbcWriterConfiguration conf) {
      super(batchCode, batchStartedAt, conf);
    }

    @Override
    protected String name() {
      return "bill-tax";
    }

    @Override
    protected void write(Connection conn, Iterator<BillTax> partition) {
      try (Query insertBillTax = createQueryWithDefaults(conn, resourceAsString("sql/outputs/insert-bill-tax.sql"));
          Query insertBillTaxDetail = createQueryWithDefaults(conn, resourceAsString("sql/outputs/insert-bill-tax-detail.sql"))) {

        bindAndExecuteStatements(
            partition,
            insertBillTax,
            insertBillTaxDetail
        );
      }
    }

    private void bindAndExecuteStatements(Iterator<BillTax> partition, Query insertBillTax, Query insertBillTaxDetail) {

      partition.forEachRemaining(billTax -> {
        bindAndAdd(billTax, insertBillTax);
        Array.of(billTax.getBillTaxDetails()).forEach(billTaxDetail -> bindAndAdd(billTax, billTaxDetail, insertBillTaxDetail));
      });

      timed("insert-bill-tax", insertBillTax::executeBatch);
      timed("insert-bill-tax-detail", insertBillTaxDetail::executeBatch);
    }

    private void bindAndAdd(BillTax billTax, Query stmt) {
      stmt.addParameter(BILL_TAX_ID_COLUMN_NAME, billTax.getBillTaxId())
          .addParameter(BILL_ID_COLUMN_NAME, billTax.getBillId())
          .addParameter("merch_tax_reg", billTax.getMerchantTaxRegistrationNumber())
          .addParameter("wp_tax_reg", billTax.getWorldpayTaxRegistrationNumber())
          .addParameter("tax_type", billTax.getTaxType())
          .addParameter("tax_authority", billTax.getTaxAuthority())
          .addParameter(REV_CHG_FLG_COLUMN_NAME, billTax.getReverseChargeFlag())
          .addToBatch();
    }

    private void bindAndAdd(BillTax billTax, BillTaxDetail billTaxDetail, Query stmt) {
      stmt.addParameter(BILL_TAX_ID_COLUMN_NAME, billTax.getBillTaxId())
          .addParameter(BILL_ID_COLUMN_NAME, billTax.getBillId())
          .addParameter("tax_stat", billTaxDetail.getTaxStatus())
          .addParameter("tax_rate", billTaxDetail.getTaxRate())
          .addParameter("tax_stat_descr", billTaxDetail.getTaxStatusDescription())
          .addParameter("net_amt", billTaxDetail.getNetAmount())
          .addParameter("tax_amt", billTaxDetail.getTaxAmount())
          .addParameter(REV_CHG_FLG_COLUMN_NAME, billTax.getReverseChargeFlag())
          .addToBatch();
    }
  }
}
