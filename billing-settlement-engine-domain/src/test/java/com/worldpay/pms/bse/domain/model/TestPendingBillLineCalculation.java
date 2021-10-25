package com.worldpay.pms.bse.domain.model;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.With;

@Data
@AllArgsConstructor
@With
@Builder
public class TestPendingBillLineCalculation implements PendingBillLineCalculation {

  private String billLineCalcId;
  private String calculationLineClassification;
  private String calculationLineType;
  private BigDecimal amount;
  private String includeOnBill;
  private String rateType;
  private BigDecimal rateValue;
}
