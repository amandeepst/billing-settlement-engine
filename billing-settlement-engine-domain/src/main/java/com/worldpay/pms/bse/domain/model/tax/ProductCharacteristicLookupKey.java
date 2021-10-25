package com.worldpay.pms.bse.domain.model.tax;

import lombok.NonNull;
import lombok.Value;

@Value
public class ProductCharacteristicLookupKey {

  @NonNull
  String productCode;
  @NonNull
  String characteristicType;

  public static ProductCharacteristicLookupKey from(ProductCharacteristic productCharacteristic) {
    return new ProductCharacteristicLookupKey(productCharacteristic.getProductCode(), productCharacteristic.getCharacteristicType());
  }
}