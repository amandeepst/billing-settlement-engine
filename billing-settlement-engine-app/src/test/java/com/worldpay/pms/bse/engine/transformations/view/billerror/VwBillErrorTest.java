package com.worldpay.pms.bse.engine.transformations.view.billerror;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.bse.engine.transformations.model.billerror.BillError;
import com.worldpay.pms.bse.engine.transformations.model.billerror.BillErrorDetail;
import com.worldpay.pms.bse.engine.transformations.view.SimpleViewTest;
import java.sql.Date;

public class VwBillErrorTest extends SimpleViewTest {

  private static final String QUERY_INSERT_BILL_ERROR = resourceAsString("sql/bill_error/insert_bill_error.sql");
  private static final String QUERY_COUNT_BILL_ERROR = resourceAsString("sql/bill_error/count_vw_bill_error.sql");

  static final BillError BILL_ERROR = createBillError();

  private static BillError createBillError() {
    return BillError.builder()
        .billId("bill_id_1")
        .billDate(Date.valueOf("2020-01-28"))
        .firstFailureOn(Date.valueOf("2020-01-28"))
        .retryCount(2)
        .billErrorDetails(new BillErrorDetail[0])
        .build();
  }

  @Override
  protected String getTableName() {
    return "bill_error";
  }

  @Override
  protected void insertEntry(String id, String batchHistoryCode, int batchAttempt) {
    sqlDb.execQuery("insert_bill_error", QUERY_INSERT_BILL_ERROR, (query) ->
        query
            .addParameter("bill_id", id)
            .addParameter("bill_dt", BILL_ERROR.getBillDate())
            .addParameter("first_failure_on", BILL_ERROR.getFirstFailureOn())
            .addParameter("retry_count", BILL_ERROR.getRetryCount())
            .addParameter("batch_code", batchHistoryCode)
            .addParameter("batch_attempt", batchAttempt)
            .addParameter("partition_id", 1)
            .addParameter("ilm_dt", GENERIC_TIMESTAMP)
            .executeUpdate());
  }

  @Override
  protected long countVwEntryByAllFields(String id) {
    return sqlDb.execQuery("count_bill_error", QUERY_COUNT_BILL_ERROR, (query) ->
        query
            .addParameter("bill_id", id)
            .addParameter("bill_dt", BILL_ERROR.getBillDate())
            .executeScalar(Long.TYPE));
  }
}
