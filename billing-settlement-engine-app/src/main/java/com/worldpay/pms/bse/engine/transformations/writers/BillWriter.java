package com.worldpay.pms.bse.engine.transformations.writers;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static com.worldpay.pms.spark.core.SparkUtils.timed;

import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcBatchPartitionWriterFunction;
import com.worldpay.pms.spark.core.jdbc.JdbcWriter;
import com.worldpay.pms.spark.core.jdbc.JdbcWriterConfiguration;
import io.vavr.collection.Array;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.sql2o.Connection;
import org.sql2o.Query;

public class BillWriter extends JdbcWriter<BillRow> {

  public static final String BILL_ID = "bill_id";
  public static final String BILL_LINE_ID = "bill_ln_id";
  public static final String TAX_STATUS = "tax_stat";
  private static final String ROOT = "sql/outputs/";

  public BillWriter(JdbcWriterConfiguration conf) {
    super(conf);
  }

  @Override
  protected JdbcBatchPartitionWriterFunction<BillRow> writer(BatchId batchId, Timestamp startedAt, JdbcWriterConfiguration conf) {
    return new Writer(batchId, startedAt, conf);
  }

  @Slf4j
  private static class Writer extends JdbcBatchPartitionWriterFunction<BillRow> {

    public Writer(BatchId batchCode, Timestamp batchStartedAt, JdbcWriterConfiguration conf) {
      super(batchCode, batchStartedAt, conf);
    }

    @Override
    protected String name() {
      return "complete-bill";
    }

    @Override
    protected void write(Connection conn, Iterator<BillRow> partition) {
      try (Query insertBill = createQueryWithDefaults(conn, resourceAsString(ROOT + "insert-bill.sql"));
          Query insertLine = createQueryWithDefaults(conn, resourceAsString(ROOT + "insert-bill-line.sql"));
          Query insertLineTransposition = createQueryWithDefaults(conn, resourceAsString(ROOT + "insert-bill-line-transposition.sql"));
          Query insertLineCalc = createQueryWithDefaults(conn, resourceAsString(ROOT + "insert-bill-line-calc.sql"));
          Query insertLineSvcQty = createQueryWithDefaults(conn, resourceAsString(ROOT + "insert-bill-line-svc-qty.sql"))) {

        bindAndExecuteStatements(
            partition,
            insertBill,
            insertLine,
            insertLineTransposition,
            insertLineCalc,
            insertLineSvcQty
        );
      }
    }

    private void bindAndExecuteStatements(Iterator<BillRow> partition, Query insertBill, Query insertLine, Query insertLineTransposition,
        Query insertLineCalc, Query insertLineSvcQty) {

      partition.forEachRemaining(billRow -> {
        bindAndAdd(billRow, insertBill);

        Array.of(billRow.getBillLines())
            .forEach(billLineRow -> {
              bindAndAdd(billRow.getBillId(), billLineRow, insertLine);

              Map<String, BigDecimal> rateMap = getRateMap(billLineRow);
              bindAndAdd(billRow.getBillId(), billLineRow, rateMap, insertLineTransposition);

              Array.of(billLineRow.getBillLineCalculations())
                  .forEach(calc -> bindAndAdd(billRow.getBillId(), billLineRow.getBillLineId(), calc, insertLineCalc));

              Array.of(billLineRow.getBillLineServiceQuantities())
                  .forEach(svcQty -> bindAndAdd(billRow.getBillId(), billLineRow.getBillLineId(), svcQty, insertLineSvcQty));
            });
      });

      timed("insert-bill", insertBill::executeBatch);
      timed("insert-bill-line", insertLine::executeBatch);
      timed("insert-bill-line-transposition", insertLineTransposition::executeBatch);
      timed("insert-bill-line-calc", insertLineCalc::executeBatch);
      timed("insert-bill-line-svc-qty", insertLineSvcQty::executeBatch);
    }

    private void bindAndAdd(BillRow billRow, Query insertBill) {
      insertBill
          .addParameter(BILL_ID, billRow.getBillId())
          .addParameter("party_id", billRow.getPartyId())
          .addParameter("bill_sub_acct_id", billRow.getBillSubAccountId())
          .addParameter("tariff_type", billRow.getTariffType())
          .addParameter("template_type", billRow.getTemplateType())
          .addParameter("lcp", billRow.getLegalCounterparty())
          .addParameter("acct_type", billRow.getAccountType())
          .addParameter("account_id", billRow.getAccountId())
          .addParameter("business_unit", billRow.getBusinessUnit())
          .addParameter("bill_dt", billRow.getBillDate())
          .addParameter("bill_cyc_id", billRow.getBillCycleId())
          .addParameter("start_dt", billRow.getStartDate())
          .addParameter("end_dt", billRow.getEndDate())
          .addParameter("currency_cd", billRow.getCurrencyCode())
          .addParameter("bill_amt", billRow.getBillAmount())
          .addParameter("bill_ref", billRow.getBillReference())
          .addParameter("status", billRow.getStatus())
          .addParameter("adhoc_bill_flg", billRow.getAdhocBillFlag())
          .addParameter("sett_sub_lvl_type", billRow.getSettlementSubLevelType())
          .addParameter("sett_sub_lvl_val", billRow.getSettlementSubLevelValue())
          .addParameter("granularity", billRow.getGranularity())
          .addParameter("granularity_key_val", billRow.getGranularityKeyValue())
          .addParameter("rel_waf_flg", billRow.getReleaseReserveIndicator())
          .addParameter("rel_reserve_flg", billRow.getReleaseReserveIndicator())
          .addParameter("fastest_pay_route", billRow.getFastestPaymentRouteIndicator())
          .addParameter("case_id", billRow.getCaseId())
          .addParameter("individual_bill", billRow.getIndividualBillIndicator())
          .addParameter("manual_narrative", billRow.getManualNarrative())
          .addParameter("settlement_region_id", billRow.getProcessingGroup())
          .addParameter("prev_bill_id", billRow.getPreviousBillId())
          .addParameter("debt_dt", billRow.getDebtDate())
          .addParameter("debt_mig_type", billRow.getDebtMigrationType())
          .addParameter("merch_tax_reg", billRow.getMerchantTaxRegistrationNumber())
          .addParameter("wp_tax_reg", billRow.getWorldpayTaxRegistrationNumber())
          .addParameter("tax_type", billRow.getTaxType())
          .addParameter("tax_authority", billRow.getTaxAuthority())
          .addToBatch();

    }

    private void bindAndAdd(String billId, BillLineRow billLineRow, Query insertLine) {
      insertLine
          .addParameter(BILL_LINE_ID, billLineRow.getBillLineId())
          .addParameter(BILL_ID, billId)
          .addParameter("bill_line_party_id", billLineRow.getBillLineParty())
          .addParameter("class", billLineRow.getProductClass())
          .addParameter("product_id", billLineRow.getProductId())
          .addParameter("product_descr", billLineRow.getProductDescription())
          .addParameter("price_ccy", billLineRow.getPricingCurrency())
          .addParameter("fund_ccy", billLineRow.getFundingCurrency())
          .addParameter("fund_amt", billLineRow.getFundingAmount())
          .addParameter("txn_ccy", billLineRow.getTransactionCurrency())
          .addParameter("txn_amt", billLineRow.getTransactionAmount())
          .addParameter("qty", billLineRow.getQuantity())
          .addParameter("price_ln_id", billLineRow.getPriceLineId())
          .addParameter("merchant_code", billLineRow.getMerchantCode())
          .addParameter("total_amount", billLineRow.getTotalAmount())
          .addParameter(TAX_STATUS, billLineRow.getTaxStatus())
          .addParameter("prev_bill_ln", billLineRow.getPreviousBillLine())
          .addToBatch();
    }

    private void bindAndAdd(String billId, BillLineRow billLineRow, Map<String, BigDecimal> rateMap, Query insertLineTransposition) {
      insertLineTransposition
          .addParameter(BILL_ID, billId)
          .addParameter(BILL_LINE_ID, billLineRow.getBillLineId())
          .addParameter("bill_line_party_id", billLineRow.getBillLineParty())
          .addParameter("class", billLineRow.getProductClass())
          .addParameter("product_id", billLineRow.getProductId())
          .addParameter("product_descr", billLineRow.getProductDescription())
          .addParameter("price_ccy", billLineRow.getPricingCurrency())
          .addParameter("fund_ccy", billLineRow.getFundingCurrency())
          .addParameter("qty", billLineRow.getQuantity())
          .addParameter("msc_pi", rateMap.getOrDefault("MSC_PI", BigDecimal.ZERO))
          .addParameter("msc_pc", rateMap.getOrDefault("MSC_PC", BigDecimal.ZERO))
          .addParameter("asf_pi", rateMap.getOrDefault("ASF_PI", BigDecimal.ZERO))
          .addParameter("asf_pc", rateMap.getOrDefault("ASF_PC", BigDecimal.ZERO))
          .addParameter("pi_amt", rateMap.getOrDefault("PI_AMT", BigDecimal.ZERO))
          .addParameter("pc_amt", rateMap.getOrDefault("PC_AMT", BigDecimal.ZERO))
          .addParameter("fund_amt", rateMap.getOrDefault("F_M_AMT", BigDecimal.ZERO))
          .addParameter("ic_amt", rateMap.getOrDefault("IC_AMT", BigDecimal.ZERO))
          .addParameter("sf_amt", rateMap.getOrDefault("SF_AMT", BigDecimal.ZERO))
          .addParameter("total_amt", billLineRow.getTotalAmount())
          .addParameter(TAX_STATUS, billLineRow.getTaxStatus())
          .addToBatch();
    }

    private void bindAndAdd(String billId, String billLineId, BillLineCalculationRow calculationRow, Query insertLineCalc) {
      insertLineCalc
          .addParameter("bill_line_calc_id", calculationRow.getBillLineCalcId())
          .addParameter(BILL_LINE_ID, billLineId)
          .addParameter(BILL_ID, billId)
          .addParameter("calc_ln_class", calculationRow.getCalculationLineClassification())
          .addParameter("calc_ln_type", calculationRow.getCalculationLineType())
          .addParameter("calc_ln_type_descr", calculationRow.getCalculationLineTypeDescription())
          .addParameter("amount", calculationRow.getAmount())
          .addParameter("include_on_bill", calculationRow.getIncludeOnBill())
          .addParameter("rate_type", calculationRow.getRateType())
          .addParameter("rate_val", calculationRow.getRateValue())
          .addParameter(TAX_STATUS, calculationRow.getTaxStatus())
          .addParameter("tax_rate", calculationRow.getTaxRate())
          .addParameter("tax_stat_descr", calculationRow.getTaxStatusDescription())
          .addToBatch();
    }

    private void bindAndAdd(String billId, String billLineId, BillLineServiceQuantityRow serviceQuantityRow, Query insertLineSvcQty) {
      insertLineSvcQty
          .addParameter(BILL_ID, billId)
          .addParameter(BILL_LINE_ID, billLineId)
          .addParameter("svc_qty_cd", serviceQuantityRow.getServiceQuantityCode())
          .addParameter("svc_qty", serviceQuantityRow.getServiceQuantity())
          .addParameter("svc_qty_type_descr", serviceQuantityRow.getServiceQuantityDescription())
          .addToBatch();
    }

    private Map<String, BigDecimal> getRateMap(BillLineRow billLineRow) {

      return Arrays.stream(billLineRow.getBillLineCalculations())
          .collect(
              Collectors.groupingBy(
                  BillLineCalculationRow::getRateType,
                  Collectors.mapping(
                      BillLineCalculationRow::getRateValue,
                      Collectors.reducing(BigDecimal.ZERO, BigDecimal::add)
                  )
              )
          );
    }


  }


}
