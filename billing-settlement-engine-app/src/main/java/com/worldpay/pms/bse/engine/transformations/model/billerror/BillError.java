package com.worldpay.pms.bse.engine.transformations.model.billerror;

import static com.worldpay.pms.bse.engine.transformations.model.FieldConstants.IGNORED_RETRY_COUNT;

import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import com.worldpay.pms.spark.core.ErrorEvent;
import java.sql.Date;
import java.time.LocalDate;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class BillError implements ErrorEvent {

  String billId;
  Date billDate;
  Date firstFailureOn;
  int retryCount;
  BillErrorDetail[] billErrorDetails;

  public static BillError fixed(BillRow bill) {
    return new BillError(bill.getBillId(), bill.getBillDate(), bill.getFirstFailureOn(), bill.getRetryCount(), null);
  }

  public static BillError error(PendingBillRow pendingBill, BillErrorDetail[] billErrorDetails) {
    return new BillError(pendingBill.getBillId(), pendingBill.getScheduleEnd(), pendingBill.getFirstFailureOn(),
        pendingBill.getRetryCount(), billErrorDetails);
  }

  public static BillError error(String billId, Date billDate,  Date firstFailureOn, int retryCount,  BillErrorDetail[] billErrorDetails){
    return new BillError(billId, billDate, firstFailureOn, retryCount, billErrorDetails);
  }

  @Override
  public boolean isIgnored() {
    return retryCount == IGNORED_RETRY_COUNT;
  }

  @Override
  public boolean isFirstFailure() {
    return getFirstFailureOn() == null || getFirstFailureOn().toLocalDate().isEqual(LocalDate.now());
  }
}