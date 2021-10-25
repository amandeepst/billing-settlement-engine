package com.worldpay.pms.bse.domain.model.billtax;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder(toBuilder = true)
public class BillLineTax {

  @NonNull
  private String taxStatus;
  @NonNull
  private BigDecimal taxRate;
  @NonNull
  private String taxStatusDescription;
  @NonNull
  private BigDecimal netAmount;
  @NonNull
  private BigDecimal taxAmount;

}
