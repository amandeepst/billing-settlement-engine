package com.worldpay.pms.bse.domain.model;

import java.math.BigDecimal;

public interface PendingBillLineServiceQuantity {

  String getServiceQuantityTypeCode();

  BigDecimal getServiceQuantityValue();
}
