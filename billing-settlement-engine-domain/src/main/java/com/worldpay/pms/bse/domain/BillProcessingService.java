package com.worldpay.pms.bse.domain;

import com.worldpay.pms.bse.domain.common.BillLineDomainError;
import com.worldpay.pms.bse.domain.model.PendingBill;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillDecision;
import com.worldpay.pms.bse.domain.model.completebill.UpdateBillDecision;
import com.worldpay.pms.bse.domain.model.mincharge.PendingMinimumCharge;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;

public interface BillProcessingService {

  Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBill(PendingBill pendingBill,
      Seq<PendingMinimumCharge> pendingMinimumCharges);

  Validation<Seq<BillLineDomainError>, UpdateBillDecision> updateBill(PendingBill pendingBill);

}
