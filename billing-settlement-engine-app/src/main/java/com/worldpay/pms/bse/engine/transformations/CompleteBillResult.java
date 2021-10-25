package com.worldpay.pms.bse.engine.transformations;

import static lombok.AccessLevel.PRIVATE;

import com.worldpay.pms.bse.domain.model.mincharge.PendingMinimumCharge;
import com.worldpay.pms.bse.engine.transformations.model.PendingOrCompleteBill;
import com.worldpay.pms.bse.engine.transformations.model.billerror.BillError;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import lombok.With;

/**
 * The bill s is either a pending bill or a complete bill .
 * In case it's pending, it can be error or not. A complete bill cannot be in error.
 * The error can also be without a pending bill
 *  - for example, error completing minimum charge bill without an input pending bill.
 * The pending minimum charges can also be without a bill / pending bill
 *  - for example, tried to create a bill for pending minimum charges without an input pending bill but it's not end of month.
 */
@AllArgsConstructor(access = PRIVATE)
@Value
public class CompleteBillResult {

  PendingOrCompleteBill bill;
  BillError billError;
  @With
  PendingMinimumCharge[] pendingMinimumCharges;

  public static CompleteBillResult pending(@NonNull PendingBillRow pendingBill, PendingMinimumCharge[] pendingMinimumCharges) {
    return new CompleteBillResult(PendingOrCompleteBill.pending(pendingBill), null, pendingMinimumCharges);
  }

  public static CompleteBillResult complete(@NonNull BillRow completeBill, PendingMinimumCharge[] pendingMinimumCharges) {
    return new CompleteBillResult(PendingOrCompleteBill.complete(completeBill), null, pendingMinimumCharges);
  }

  public static CompleteBillResult error(@NonNull BillError billError, PendingBillRow pendingBill) {
    PendingOrCompleteBill bill = pendingBill == null ? null : PendingOrCompleteBill.pending(pendingBill);
    return new CompleteBillResult(bill, billError, null);
  }

  public static CompleteBillResult pendingMinimumCharges(@NonNull PendingMinimumCharge[] pendingMinimumCharges) {
    return new CompleteBillResult(null, null, pendingMinimumCharges);
  }

  public PendingBillRow getPendingBill() {
    return isPending() ? bill.getPendingBill() : null;
  }

  public BillRow getCompleteBill() {
    return isComplete() ? bill.getCompleteBill() : null;
  }

  public BillError getBillError() {
    return billError;
  }

  public PendingMinimumCharge[] getPendingMinimumCharges() {
    return pendingMinimumCharges;
  }

  public boolean isPending() {
    return bill != null && bill.isPending();
  }

  public boolean isComplete() {
    return bill != null && bill.isComplete();
  }

  public boolean isError() {
    return billError != null;
  }

  public boolean isPendingMinimumCharges() {
    return pendingMinimumCharges != null && pendingMinimumCharges.length > 0;
  }

  public boolean isNotEmpty() {
    return isPending() || isComplete() || isError() || isPendingMinimumCharges();
  }
}