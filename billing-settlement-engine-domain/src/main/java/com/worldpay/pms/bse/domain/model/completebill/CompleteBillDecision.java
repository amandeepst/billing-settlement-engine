package com.worldpay.pms.bse.domain.model.completebill;

import com.worldpay.pms.bse.domain.model.mincharge.PendingMinimumCharge;
import lombok.Value;

@Value
public class CompleteBillDecision {

  CompleteBill completeBill;
  boolean shouldComplete;

  PendingMinimumCharge[] pendingMinimumCharges;

  public static CompleteBillDecision doNotCompleteDecision() {
    return doNotCompleteDecision(null);
  }

  public static CompleteBillDecision doNotCompleteDecision(PendingMinimumCharge[] pendingMinimumCharges){
    return new CompleteBillDecision(null, false, pendingMinimumCharges);
  }

  public static CompleteBillDecision completeDecision(CompleteBill completeBill, PendingMinimumCharge[] pendingMinimumCharges) {
    return new CompleteBillDecision(completeBill, true, pendingMinimumCharges);
  }
}
