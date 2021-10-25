package com.worldpay.pms.bse.domain.model;

import java.math.BigDecimal;
import java.util.Map;

public interface BillableItemServiceQuantity {

  String getRateSchedule();

  Map<String, BigDecimal> getServiceQuantities();
}
