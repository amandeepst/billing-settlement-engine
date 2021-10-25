package com.worldpay.pms.bse.engine.transformations.model.input.misc;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MiscBillableItemLineRow {

  @NonNull
  private String billableItemId;

  private String lineCalculationType;

  private BigDecimal amount;

  private BigDecimal price;
  private int partitionId;
}
