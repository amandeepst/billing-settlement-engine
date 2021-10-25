package com.worldpay.pms.bse.engine.transformations.model.input.correction;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class InputBillLineCalcCorrectionRow {

  @NonNull
  private String billId;
  private String billLineId;
  private String billLineCalcId;
  private String calculationLineClassification;
  private String calculationLineType;
  private String calculationLineTypeDescription;
  private BigDecimal amount;
  private String includeOnBill;
  private String rateType;
  private BigDecimal rateValue;
  private String taxStatus;
  private BigDecimal taxRate;
  private String taxStatusDescription;
  private long partitionId;
}
