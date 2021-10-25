package com.worldpay.pms.bse.domain.model;

import java.math.BigDecimal;

public interface BillableItemLine {

  public String getCalculationLineClassification();

  public BigDecimal getAmount();

  public String getCalculationLineType();

  public String getRateType();

  public BigDecimal getRateValue();
}
