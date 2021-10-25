package com.worldpay.pms.bse.domain.model.mincharge;

import com.worldpay.pms.bse.domain.model.completebill.CompleteBill;
import io.vavr.collection.Seq;
import lombok.Value;

@Value
public class MinimumChargeResult {

  CompleteBill completeBill;
  Seq<PendingMinimumCharge> pendingMinimumCharges;

}
