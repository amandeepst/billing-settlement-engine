package com.worldpay.pms.bse.engine.transformations.model.input.pending;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class InputPendingBillLineCalculationRow {

  private String billLineCalcId;
  private String billLineId;
  private String billId;
  private String calculationLineClassification;
  private String calculationLineType;
  private BigDecimal amount;
  private String includeOnBill;
  private String rateType;
  private BigDecimal rateValue;
  private long partitionId;
}
