package com.worldpay.pms.bse.domain.model;

import java.math.BigDecimal;

public interface BillLineServiceQuantity {


  String getServiceQuantityCode();

  BigDecimal getServiceQuantity();

  String getServiceQuantityDescription();

}
