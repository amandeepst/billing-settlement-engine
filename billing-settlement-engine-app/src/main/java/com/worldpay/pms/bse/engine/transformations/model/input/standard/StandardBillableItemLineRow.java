package com.worldpay.pms.bse.engine.transformations.model.input.standard;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StandardBillableItemLineRow {

  @NonNull
  private String billableItemId;
  private String distributionId;

  private BigDecimal preciseChargeAmount;
  private String characteristicValue;

  private String rateType;

  private BigDecimal rate;
  private int partitionId;

}
