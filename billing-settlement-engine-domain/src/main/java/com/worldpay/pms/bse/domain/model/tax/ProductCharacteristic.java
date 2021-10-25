package com.worldpay.pms.bse.domain.model.tax;

import lombok.NonNull;
import lombok.Value;

@Value
public class ProductCharacteristic {

  @NonNull
  String productCode;
  @NonNull
  String characteristicType;
  @NonNull
  String characteristicValue;
}
