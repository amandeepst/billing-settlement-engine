package com.worldpay.pms.bse.engine.transformations.model.billableitem;

import com.worldpay.pms.bse.domain.model.BillableItemServiceQuantity;
import java.math.BigDecimal;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class BillableItemServiceQuantityRow implements BillableItemServiceQuantity {

  private String rateSchedule;
  @NonNull
  private Map<String, BigDecimal> serviceQuantities;

}
