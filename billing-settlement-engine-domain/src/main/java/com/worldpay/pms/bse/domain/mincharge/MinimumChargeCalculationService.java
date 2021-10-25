package com.worldpay.pms.bse.domain.mincharge;

import com.worldpay.pms.bse.domain.account.BillingAccount;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBill;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumChargeResult;
import com.worldpay.pms.bse.domain.model.mincharge.PendingMinimumCharge;
import io.vavr.collection.Seq;

public interface MinimumChargeCalculationService {

  MinimumChargeResult compute(CompleteBill bill, Seq<PendingMinimumCharge> pendingMinimumCharges);

  Boolean shouldCompleteAnyPending(Seq<PendingMinimumCharge> pendingMinimumCharges);

  MinimumChargeResult compute(BillingAccount account, Seq<PendingMinimumCharge> pendingMinimumCharges);

  Seq<PendingMinimumCharge> getPendingMinCharges(Seq<PendingMinimumCharge> pending);
}
