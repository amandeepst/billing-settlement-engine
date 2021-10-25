package com.worldpay.pms.pba.domain;

import com.worldpay.pms.pba.domain.model.Bill;
import com.worldpay.pms.pba.domain.model.BillAdjustment;
import io.vavr.collection.List;

public interface WafProcessingService {

  List<BillAdjustment> computeAdjustment(List<Bill> bills);

}
