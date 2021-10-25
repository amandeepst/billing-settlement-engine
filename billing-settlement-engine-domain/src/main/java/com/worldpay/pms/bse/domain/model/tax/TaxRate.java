package com.worldpay.pms.bse.domain.model.tax;

import java.math.BigDecimal;
import lombok.NonNull;
import lombok.Value;

@Value
public class TaxRate {

  @NonNull
  String taxStatusType;
  @NonNull
  String taxStatusValue;
  @NonNull
  String taxStatusCode;
  @NonNull
  String taxStatusDescription;
  @NonNull
  BigDecimal taxRate;

}
