package com.worldpay.pms.bse.domain.model;

import java.math.BigDecimal;

public interface BillLineCalculation {

  String getBillLineCalcId();

  String getCalculationLineClassification();

  String getCalculationLineType();

  String getCalculationLineTypeDescription();

  BigDecimal getAmount();

  String getIncludeOnBill();

  String getRateType();

  BigDecimal getRateValue();

  String getTaxStatus();

  BigDecimal getTaxRate();

  String getTaxStatusDescription();
}
