package com.worldpay.pms.bse.engine.transformations;

import com.worldpay.pms.bse.engine.transformations.model.billerror.BillError;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import lombok.NonNull;
import lombok.Value;

@Value
public class UpdateBillResult {

  @NonNull
  PendingBillRow pendingBill;
  BillError billError;

  private UpdateBillResult(PendingBillRow pendingBill, BillError billError) {
    this.pendingBill = pendingBill;
    this.billError = billError;
  }

  public static UpdateBillResult success(@NonNull PendingBillRow pendingBill) {
    return new UpdateBillResult(pendingBill, null);
  }

  public static UpdateBillResult error(@NonNull PendingBillRow pendingBill, @NonNull BillError billError) {
    return new UpdateBillResult(pendingBill, billError);
  }

  public boolean isSuccess() {
    return !isError();
  }

  public boolean isError() {
    return billError != null;
  }
}