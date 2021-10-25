package com.worldpay.pms.bse.engine.transformations.view;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.domain.model.BillLineServiceQuantity;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineServiceQuantityRow;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class VwInvoiceDataLineSvcQtyTest extends SimpleViewTest {

  private static final String QUERY_INSERT_BILL_LINE_SVC_QTY = resourceAsString(
      "sql/vw_invoice_data_line_svc_qty/insert_bill_line_svc_qty.sql");
  private static final String QUERY_COUNT_VW_INVOICE_DATA_LINE_SVC_QTY = resourceAsString(
      "sql/vw_invoice_data_line_svc_qty/count_vw_invoice_data_line_svc_qty_by_all_fields.sql");

  private final BillLineServiceQuantity genericBillLineServiceQuantity = new BillLineServiceQuantityRow("TXN_VOL", BigDecimal.TEN,
      "transaction volume");

  @Override
  protected String getTableName() {
    return "bill_line_svc_qty";
  }

  @Override
  protected void insertEntry(String id, String batchHistoryCode, int batchAttempt) {
    insertEntry(genericBillLineServiceQuantity, id, batchHistoryCode, batchAttempt);
  }

  protected void insertEntry(BillLineServiceQuantity billLineServiceQuantity, String id, String batchHistoryCode, int batchAttempt) {
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

  @Override
  protected long countVwEntryByAllFields(String id) {
    return countVwEntryByAllFields(genericBillLineServiceQuantity, id);
  }

  protected long countVwEntryByAllFields(BillLineServiceQuantity billLineServiceQuantity, String id) {
    return sqlDb.execQuery("count_vw_invoice_data_line_svc_qty", QUERY_COUNT_VW_INVOICE_DATA_LINE_SVC_QTY, (query) ->
        query
            .addParameter("bill_id", id)
            .addParameter("bill_ln_id", "line_" + id)
            .addParameter("svc_qty_cd", billLineServiceQuantity.getServiceQuantityCode())
            .addParameter("svc_qty", billLineServiceQuantity.getServiceQuantity())
            .addParameter("svc_qty_type_descr", billLineServiceQuantity.getServiceQuantityDescription())
            .executeScalar(Long.TYPE));
  }

  @Test
  void whenBillLineServiceQuantitiesAreInsertedThenOnlyTheOnesWithSelectedCodeAreVisibleInTheView() {

    BillLineServiceQuantityRow billLineServiceQuantity2 = new BillLineServiceQuantityRow("TXN_AMT", BigDecimal.TEN, "transaction amt");
    BillLineServiceQuantityRow billLineServiceQuantity3 = new BillLineServiceQuantityRow("F_M_AMT", BigDecimal.TEN, "fund merch amt");
    BillLineServiceQuantityRow billLineServiceQuantity4 = new BillLineServiceQuantityRow("AP_B_EFX", BigDecimal.ONE, "fx rate");
    BillLineServiceQuantityRow billLineServiceQuantity5 = new BillLineServiceQuantityRow("P_B_EFX", BigDecimal.ONE, "fx rate");

    String batchCode = "b_1";
    int batchAttempt = 2;

    insertBatchHistoryAndOnBatchCompleted(batchCode, batchAttempt, STATE_COMPLETED);
    insertEntry(genericBillLineServiceQuantity, "abc1", batchCode, batchAttempt);
    insertEntry(billLineServiceQuantity2, "abc2", batchCode, batchAttempt);
    insertEntry(billLineServiceQuantity3, "abc3", batchCode, batchAttempt);
    insertEntry(billLineServiceQuantity4, "abc4", batchCode, batchAttempt);
    insertEntry(billLineServiceQuantity5, "abc5", batchCode, batchAttempt);

    assertVwInvoiceDataLineSvcQtyEntryExists(genericBillLineServiceQuantity, "abc1");
    assertVwInvoiceDataLineSvcQtyEntryExists(billLineServiceQuantity2, "abc2");
    assertVwInvoiceDataLineSvcQtyEntryExists(billLineServiceQuantity3, "abc3");
    assertVwInvoiceDataLineSvcQtyEntryNotExists(billLineServiceQuantity4, "abc4");
    assertVwInvoiceDataLineSvcQtyEntryNotExists(billLineServiceQuantity5, "abc5");
  }

  void assertVwInvoiceDataLineSvcQtyEntryExists(BillLineServiceQuantity billLineServiceQuantity, String id) {
    long count = countVwEntryByAllFields(billLineServiceQuantity, id);
    assertThat(count, is(1L));
  }

  void assertVwInvoiceDataLineSvcQtyEntryNotExists(BillLineServiceQuantity billLineServiceQuantity, String id) {
    long count = countVwEntryByAllFields(billLineServiceQuantity, id);
    assertThat(count, is(0L));
  }
}
