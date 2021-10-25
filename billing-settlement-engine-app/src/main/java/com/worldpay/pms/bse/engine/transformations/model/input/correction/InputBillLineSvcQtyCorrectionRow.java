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
public class InputBillLineSvcQtyCorrectionRow {

  @NonNull
  private String billId;
  private String billLineId;
  private String serviceQuantityCode;
  private BigDecimal serviceQuantity;
  private String serviceQuantityDescription;
  private long partitionId;
}
