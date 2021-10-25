package com.worldpay.pms.bse.engine.transformations.view.billerror;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.bse.engine.transformations.model.billerror.BillErrorDetail;
import com.worldpay.pms.bse.engine.transformations.view.SimpleViewTest;

public class VwBillErrorDetailTest extends SimpleViewTest {

  private static final String QUERY_INSERT_BILL_ERROR_DETAIL = resourceAsString("sql/bill_error_detail/insert_bill_error_detail.sql");
  private static final String QUERY_COUNT_BILL_ERROR_DETAIL = resourceAsString("sql/bill_error_detail/count_vw_bill_error_detail.sql");

  static final BillErrorDetail BILL_ERROR_DETAIL = createBillErrorDetail();

  private static BillErrorDetail createBillErrorDetail() {
    return BillErrorDetail.builder()
        .billLineId("bill_line_id_1")
        .code("error_code")
        .message("error message")
        .stackTrace("")
        .build();
  }

  @Override
  protected String getTableName() {
    return "bill_error_detail";
  }

  @Override
  protected void insertEntry(String id, String batchHistoryCode, int batchAttempt) {
    sqlDb.execQuery("insert_bill_error_detail", QUERY_INSERT_BILL_ERROR_DETAIL, (query) ->
        query
            .addParameter("bill_id", id)
            .addParameter("bill_ln_id", BILL_ERROR_DETAIL.getBillLineId())
            .addParameter("code", BILL_ERROR_DETAIL.getCode())
            .addParameter("reason", BILL_ERROR_DETAIL.getMessage())
            .addParameter("stack_trace", BILL_ERROR_DETAIL.getStackTrace())
            .addParameter("batch_code", batchHistoryCode)
            .addParameter("batch_attempt", batchAttempt)
            .addParameter("partition_id", 1)
            .addParameter("ilm_dt", GENERIC_TIMESTAMP)
            .executeUpdate());

  }

  @Override
  protected long countVwEntryByAllFields(String id) {
    return sqlDb.execQuery("count_bill_error_detail", QUERY_COUNT_BILL_ERROR_DETAIL, (query) ->
        query
            .addParameter("bill_id", id)
            .addParameter("bill_ln_id", BILL_ERROR_DETAIL.getBillLineId())
            .addParameter("error_cd", BILL_ERROR_DETAIL.getCode())
            .addParameter("error_info", BILL_ERROR_DETAIL.getMessage())
            .executeScalar(Long.TYPE));
  }
}
