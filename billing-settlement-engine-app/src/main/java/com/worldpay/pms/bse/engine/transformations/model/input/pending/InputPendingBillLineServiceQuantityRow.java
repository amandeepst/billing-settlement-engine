package com.worldpay.pms.bse.engine.transformations.model.input.pending;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class InputPendingBillLineServiceQuantityRow {

  private String billId;
  private String billLineId;
  private String serviceQuantityTypeCode;
  private BigDecimal serviceQuantityValue;
  private long partitionId;
}
