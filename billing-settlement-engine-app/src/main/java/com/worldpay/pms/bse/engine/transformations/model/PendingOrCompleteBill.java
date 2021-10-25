package com.worldpay.pms.bse.engine.transformations.model;

import static lombok.AccessLevel.PRIVATE;

import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;

/**
 * Wrapper for either a pending or a complete bill. One of the 2 has to be not null, but not both.
 */
@AllArgsConstructor(access = PRIVATE)
@Value
public class PendingOrCompleteBill {

  PendingBillRow pendingBill;
  BillRow completeBill;

  public static PendingOrCompleteBill pending(@NonNull PendingBillRow pendingBill) {
    return new PendingOrCompleteBill(pendingBill, null);
  }

  public static PendingOrCompleteBill complete(@NonNull BillRow completeBill) {
    return new PendingOrCompleteBill(null, completeBill);
  }

  public boolean isPending() {
    return pendingBill != null;
  }

  public boolean isComplete() {
    return completeBill != null;
  }
}
