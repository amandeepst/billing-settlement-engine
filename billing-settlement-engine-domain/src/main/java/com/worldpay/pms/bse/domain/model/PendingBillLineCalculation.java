package com.worldpay.pms.bse.domain.model;

import java.math.BigDecimal;

public interface PendingBillLineCalculation {

  String getBillLineCalcId();

  String getCalculationLineClassification();

  String getCalculationLineType();

  BigDecimal getAmount();

  String getIncludeOnBill();

  String getRateType();

  BigDecimal getRateValue();
}
