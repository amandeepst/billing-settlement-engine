package com.worldpay.pms.pba.domain.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BillAdjustment {

  @NonNull
  private Adjustment adjustment;

  @NonNull
  private Bill bill;

}
