package com.worldpay.pms.bse.engine.transformations.model.input.standard;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StandardBillableItemSvcQtyRow {

  @NonNull
  private String billableItemId;
  @NonNull
  private String rateSchedule;
  @NonNull
  private String sqiCd;
  @NonNull
  private BigDecimal serviceQuantity;
  private int partitionId;
}
