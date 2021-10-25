package com.worldpay.pms.bse.domain.model.billtax;

import java.math.BigDecimal;
import java.util.function.BiFunction;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder(toBuilder = true)
public class BillTaxDetail {

  private String billTaxId;
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

  public static BillTaxDetail from(String billTaxId, BillLineTax billLineTax, String currency,
      BiFunction<BigDecimal, String, BigDecimal> rounder) {
    return new BillTaxDetail(billTaxId, billLineTax.getTaxStatus(), billLineTax.getTaxRate(), billLineTax.getTaxStatusDescription(),
        billLineTax.getNetAmount(), rounder.apply(billLineTax.getTaxAmount(), currency));
  }

}
