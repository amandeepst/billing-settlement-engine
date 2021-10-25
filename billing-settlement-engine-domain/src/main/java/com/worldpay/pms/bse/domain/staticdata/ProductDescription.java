package com.worldpay.pms.bse.domain.staticdata;

import lombok.NonNull;
import lombok.Value;

@Value
public class ProductDescription {

  @NonNull
  String productCode;
  @NonNull
  String description;

}
