package com.worldpay.pms.bse.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.SparkUtils.timed;

import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
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

public class PendingBillWriter extends JdbcWriter<PendingBillRow> {

  private static final String PENDING_BILL_ID_COL_NAME = "bill_id";
  private static final String PENDING_BILL_LINE_ID_COL_NAME = "bill_ln_id";

  public PendingBillWriter(JdbcWriterConfiguration conf) {
    super(conf);
  }

  @Override
  protected JdbcBatchPartitionWriterFunction<PendingBillRow> writer(BatchId batchId, Timestamp timestamp, JdbcWriterConfiguration conf) {
    return new Writer(batchId, timestamp, conf);
  }

  @Slf4j
  private static class Writer extends JdbcBatchPartitionWriterFunction<PendingBillRow> {

    public Writer(BatchId batchCode, Timestamp batchStartedAt, JdbcWriterConfiguration conf) {
      super(batchCode, batchStartedAt, conf);
    }

    @Override
    protected String name() {
      return "pending-bill";
    }

    @Override
    protected void write(Connection conn, Iterator<PendingBillRow> partition) {
      try (Query insertHeader = createQueryWithDefaults(conn, resourceAsString("sql/outputs/insert-pending-bill.sql"));
          Query insertLine = createQueryWithDefaults(conn, resourceAsString("sql/outputs/insert-pending-bill-line.sql"));
          Query insertLineCalc = createQueryWithDefaults(conn, resourceAsString("sql/outputs/insert-pending-bill-line-calc.sql"));
          Query insertLineSvcQty = createQueryWithDefaults(conn, resourceAsString("sql/outputs/insert-pending-bill-line-svc-qty.sql"))) {

        bindAndExecuteStatements(
            partition,
            insertHeader,
            insertLine,
            insertLineCalc,
            insertLineSvcQty
        );
      }
    }

    private void bindAndExecuteStatements(Iterator<PendingBillRow> partition, Query insertHeader, Query insertLine, Query insertLineCalc,
        Query insertLineSvcQty) {

      partition.forEachRemaining(pendingBillRow -> {
        bindAndAdd(pendingBillRow, insertHeader);

        Array.of(pendingBillRow.getPendingBillLines())
            .forEach(pendingBillLine -> {
              bindAndAdd(pendingBillRow.getBillId(), pendingBillLine, insertLine);

              Array.of(pendingBillLine.getPendingBillLineCalculations())
                  .forEach(calc -> bindAndAdd(pendingBillRow.getBillId(), pendingBillLine.getBillLineId(), calc, insertLineCalc));

              Array.of(pendingBillLine.getPendingBillLineServiceQuantities())
                  .forEach(svcQty -> bindAndAdd(pendingBillRow.getBillId(), pendingBillLine.getBillLineId(), svcQty, insertLineSvcQty));
            });
      });

      timed("insert-pending-bill", insertHeader::executeBatch);
      timed("insert-pending-bill-line", insertLine::executeBatch);
      timed("insert-pending-bill-line-calc", insertLineCalc::executeBatch);
      timed("insert-pending-bill-line-svc-qty", insertLineSvcQty::executeBatch);
    }

    private void bindAndAdd(PendingBillRow row, Query stmt) {
      stmt.addParameter(PENDING_BILL_ID_COL_NAME, row.getBillId())
          .addParameter("party_id", row.getPartyId())
          .addParameter("lcp", row.getLegalCounterpartyId())
          .addParameter("acct_id", row.getAccountId())
          .addParameter("bill_sub_acct_id", row.getBillSubAccountId())
          .addParameter("acct_type", row.getAccountType())
          .addParameter("business_unit", row.getBusinessUnit())
          .addParameter("bill_cyc_id", row.getBillCycleId())
          .addParameter("start_dt", row.getScheduleStart())
          .addParameter("end_dt", row.getScheduleEnd())
          .addParameter("currency_cd", row.getCurrency())
          .addParameter("bill_ref", row.getBillReference())
          .addParameter("adhoc_bill", row.getAdhocBillIndicator())
          .addParameter("sett_sub_lvl_type", row.getSettlementSubLevelType())
          .addParameter("sett_sub_lvl_val", row.getSettlementSubLevelValue())
          .addParameter("granularity", row.getGranularity())
          .addParameter("granularity_key_val", row.getGranularityKeyValue())
          .addParameter("debt_dt", row.getDebtDate())
          .addParameter("debt_mig_type", row.getDebtMigrationType())
          .addParameter("overpayment_flg", row.getOverpaymentIndicator())
          .addParameter("rel_waf_flg", row.getReleaseWAFIndicator())
          .addParameter("rel_reserve_flg", row.getReleaseReserveIndicator())
          .addParameter("fastest_pay_route", row.getFastestPaymentRouteIndicator())
          .addParameter("case_id", row.getCaseIdentifier())
          .addParameter("individual_bill", row.getIndividualBillIndicator())
          .addParameter("manual_narrative", row.getManualBillNarrative())
          .addParameter("miscalculation_flag", row.getMiscalculationFlag())
          .addParameter("first_failure_on", row.getFirstFailureOn())
          .addParameter("retry_count", row.getRetryCount())
          .addToBatch();
    }

    private void bindAndAdd(String billId, PendingBillLineRow row, Query stmt) {
      stmt.addParameter(PENDING_BILL_LINE_ID_COL_NAME, row.getBillLineId())
          .addParameter(PENDING_BILL_ID_COL_NAME, billId)
          .addParameter("bill_line_party_id", row.getBillLinePartyId())
          .addParameter("product_class", row.getProductClass())
          .addParameter("product_id", row.getProductIdentifier())
          .addParameter("price_ccy", row.getPricingCurrency())
          .addParameter("fund_ccy", row.getFundingCurrency())
          .addParameter("fund_amt", row.getFundingAmount())
          .addParameter("txn_ccy", row.getTransactionCurrency())
          .addParameter("txn_amt", row.getTransactionAmount())
          .addParameter("qty", row.getQuantity())
          .addParameter("price_ln_id", row.getPriceLineId())
          .addParameter("merchant_code", row.getMerchantCode())
          .addToBatch();
    }

    private void bindAndAdd(String billId, String billLineId, PendingBillLineCalculationRow row, Query stmt) {
      stmt.addParameter(PENDING_BILL_LINE_ID_COL_NAME, billLineId)
          .addParameter(PENDING_BILL_ID_COL_NAME, billId)
          .addParameter("bill_line_calc_id", row.getBillLineCalcId())
          .addParameter("calc_ln_class", row.getCalculationLineClassification())
          .addParameter("calc_ln_type", row.getCalculationLineType())
          .addParameter("amount", row.getAmount())
          .addParameter("include_on_bill", row.getIncludeOnBill())
          .addParameter("rate_type", row.getRateType())
          .addParameter("rate_val", row.getRateValue())
          .addToBatch();
    }

    private void bindAndAdd(String billId, String billLineId, PendingBillLineServiceQuantityRow row, Query stmt) {
      stmt.addParameter(PENDING_BILL_LINE_ID_COL_NAME, billLineId)
          .addParameter(PENDING_BILL_ID_COL_NAME, billId)
          .addParameter("svc_qty_cd", row.getServiceQuantityTypeCode())
          .addParameter("svc_qty", row.getServiceQuantityValue())
          .addToBatch();
    }
  }
}
