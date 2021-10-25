package com.worldpay.pms.bse.domain.model.tax;

import lombok.NonNull;
import lombok.Value;

@Value
public class TaxRateLookupKey {

  @NonNull
  String taxStatusType;
  @NonNull
  String taxStatusValue;

  public static TaxRateLookupKey from(TaxRate taxRate) {
    return new TaxRateLookupKey(taxRate.getTaxStatusType(), taxRate.getTaxStatusValue());
  }
}
