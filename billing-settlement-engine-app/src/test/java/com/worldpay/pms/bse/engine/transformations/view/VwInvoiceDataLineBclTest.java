package com.worldpay.pms.bse.engine.transformations.view;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.domain.model.BillLineCalculation;
import com.worldpay.pms.bse.domain.model.BillLineServiceQuantity;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineServiceQuantityRow;
import com.worldpay.pms.testing.utils.DbUtils;
import java.math.BigDecimal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class VwInvoiceDataLineBclTest extends ViewBaseTest {

  private static final String QUERY_INSERT_BILL_LINE_CALC = resourceAsString(
      "sql/vw_invoice_data_line_bcl/insert_bill_line_calc.sql");
  private static final String QUERY_INSERT_BILL_LINE_SVC_QTY = resourceAsString(
      "sql/vw_invoice_data_line_svc_qty/insert_bill_line_svc_qty.sql");
  private static final String QUERY_COUNT_VW_INVOICE_DATA_LINE_BCL = resourceAsString(
      "sql/vw_invoice_data_line_bcl/count_vw_invoice_data_line_bcl_by_id.sql");

  @BeforeEach
  void setUp() {
    DbUtils.cleanUp(sqlDb, "bill_line_calc", "bill_line_svc_qty");
  }

  private void insertBillLineCalculationEntry(BillLineCalculation billLineCalculation, String id, String batchHistoryCode,
      int batchAttempt) {
    sqlDb.execQuery("insert_bill_line_calc", QUERY_INSERT_BILL_LINE_CALC, (query) ->
        query
            .addParameter("bill_line_calc_id", billLineCalculation.getBillLineCalcId())
            .addParameter("bill_id", id)
            .addParameter("bill_ln_id", "line_" + id)
            .addParameter("calc_ln_class", billLineCalculation.getCalculationLineClassification())
            .addParameter("calc_ln_type", billLineCalculation.getCalculationLineType())
            .addParameter("calc_ln_type_descr", billLineCalculation.getCalculationLineTypeDescription())
            .addParameter("amount", billLineCalculation.getAmount())
            .addParameter("include_on_bill", billLineCalculation.getIncludeOnBill())
            .addParameter("rate_type", billLineCalculation.getRateType())
            .addParameter("rate_val", billLineCalculation.getRateValue())
            .addParameter("tax_stat", billLineCalculation.getTaxStatus())
            .addParameter("tax_rate", billLineCalculation.getTaxRate())
            .addParameter("tax_stat_descr", billLineCalculation.getTaxStatusDescription())
            .addParameter("batch_code", batchHistoryCode)
            .addParameter("batch_attempt", batchAttempt)
            .addParameter("partition_id", 1)
            .addParameter("ilm_dt", GENERIC_TIMESTAMP)
            .executeUpdate());
  }

  private void insertBillLineSvcQtyEntry(BillLineServiceQuantity billLineServiceQuantity, String id, String batchHistoryCode,
      int batchAttempt) {
    sqlDb.execQuery("insert_bill_line_svc_qty", QUERY_INSERT_BILL_LINE_SVC_QTY, (query) ->
        query
            .addParameter("bill_id", id)
            .addParameter("bill_ln_id", "line_" + id)
            .addParameter("svc_qty_cd", billLineServiceQuantity.getServiceQuantityCode())
            .addParameter("svc_qty", billLineServiceQuantity.getServiceQuantity())
            .addParameter("svc_qty_type_descr", billLineServiceQuantity.getServiceQuantityDescription())
            .addParameter("batch_code", batchHistoryCode)
            .addParameter("batch_attempt", batchAttempt)
            .addParameter("partition_id", 1)
            .addParameter("ilm_dt", GENERIC_TIMESTAMP)
            .executeUpdate());
  }

  protected long countVwEntryById(String id) {
    return sqlDb.execQuery("count_vw_invoice_data_line_svc_qty", QUERY_COUNT_VW_INVOICE_DATA_LINE_BCL, (query) ->
        query
            .addParameter("bill_id", id)
            .executeScalar(Long.TYPE));
  }


  BillLineCalculationRow validBillLineCalculation = new BillLineCalculationRow("billCalcId", "calcLnClass", "calcLnType", "calcLineDescr",
      BigDecimal.ONE, "Y", "TXN_AMT", BigDecimal.ONE, "EXE", null, "taxDescription");
  BillLineCalculationRow invalidBillLineCalculation = new BillLineCalculationRow("billCalcId", "calcLnClass", "calcLnType", "calcLineDescr",
      BigDecimal.ONE, "N", "TXN_AMT", BigDecimal.ONE, "EXE", null, "taxDescription");
  BillLineServiceQuantityRow validBillLineServiceQuantity = new BillLineServiceQuantityRow("F_M_AMT", BigDecimal.TEN, "fund merch amt");
  BillLineServiceQuantityRow invalidBillLineServiceQuantity = new BillLineServiceQuantityRow("AP_B_EFX", BigDecimal.ONE, "fx rate");

  String batchCode = "b_1";
  int batchAttempt = 1;

  @Test
  void oneValidLineCalcAndOneValidSvcQty() {
    insertBatchHistoryAndOnBatchCompleted(batchCode, batchAttempt, STATE_COMPLETED);
    insertBillLineCalculationEntry(validBillLineCalculation, "1a", batchCode, batchAttempt);
    insertBillLineSvcQtyEntry(validBillLineServiceQuantity, "1b", batchCode, batchAttempt);

    assertVwInvoiceDataLineSvcQtyEntryExists("1a");
    assertVwInvoiceDataLineSvcQtyEntryExists("1b");
  }

  @Test
  void oneValidLineCalcAndOneInValidSvcQty() {
    insertBatchHistoryAndOnBatchCompleted(batchCode, batchAttempt, STATE_COMPLETED);
    insertBillLineCalculationEntry(validBillLineCalculation, "2a", batchCode, batchAttempt);
    insertBillLineSvcQtyEntry(invalidBillLineServiceQuantity, "2b", batchCode, batchAttempt);

    assertVwInvoiceDataLineSvcQtyEntryExists("2a");
    assertVwInvoiceDataLineSvcQtyEntryNotExists("2b");
  }

  @Test
  void oneInValidLineCalcAndOneValidSvcQty() {
    insertBatchHistoryAndOnBatchCompleted(batchCode, batchAttempt, STATE_COMPLETED);
    insertBillLineCalculationEntry(invalidBillLineCalculation, "3a", batchCode, batchAttempt);
    insertBillLineSvcQtyEntry(validBillLineServiceQuantity, "3b", batchCode, batchAttempt);

    assertVwInvoiceDataLineSvcQtyEntryNotExists("3a");
    assertVwInvoiceDataLineSvcQtyEntryExists("3b");
  }

  @Test
  void oneInValidLineCalcAndOneInValidSvcQty() {
    insertBatchHistoryAndOnBatchCompleted(batchCode, batchAttempt, STATE_COMPLETED);
    insertBillLineCalculationEntry(invalidBillLineCalculation, "4a", batchCode, batchAttempt);
    insertBillLineSvcQtyEntry(invalidBillLineServiceQuantity, "4b", batchCode, batchAttempt);

    assertVwInvoiceDataLineSvcQtyEntryNotExists("4a");
    assertVwInvoiceDataLineSvcQtyEntryNotExists("4b");
  }

  @Test
  void whenBatchIsFailedThenNoEntryIsReturned() {
    insertBatchHistoryAndOnBatchCompleted(batchCode, batchAttempt, STATE_FAILED);
    insertBillLineCalculationEntry(validBillLineCalculation, "5", batchCode, batchAttempt);

    assertVwInvoiceDataLineSvcQtyEntryNotExists("5");
  }

  ///

  void assertVwInvoiceDataLineSvcQtyEntryExists(String id) {
    long count = countVwEntryById(id);
    assertThat(count, is(1L));
  }

  void assertVwInvoiceDataLineSvcQtyEntryNotExists(String id) {
    long count = countVwEntryById(id);
    assertThat(count, is(0L));
  }

}
