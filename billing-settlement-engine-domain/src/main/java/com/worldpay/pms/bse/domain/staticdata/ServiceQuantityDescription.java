package com.worldpay.pms.bse.domain.staticdata;

import lombok.NonNull;
import lombok.Value;

@Value
public class ServiceQuantityDescription {

  @NonNull
  String serviceQuantityCode;
  @NonNull
  String description;


}
