package com.worldpay.pms.bse.domain.model.tax;

import lombok.NonNull;
import lombok.Value;

@Value
public class Country {

  @NonNull
  String countryId;
  @NonNull
  String currencyId;
  @NonNull
  String name;
  //@NonNull
  String euGeographicCode;
  String stateBasedTax;
  @NonNull
  String eEEAFlag;

}
