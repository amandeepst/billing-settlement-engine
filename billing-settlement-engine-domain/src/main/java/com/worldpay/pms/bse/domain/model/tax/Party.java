package com.worldpay.pms.bse.domain.model.tax;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@AllArgsConstructor
@Builder(toBuilder = true)
public class Party {

  @NonNull
  String partyId;
  @NonNull
  String countryId;
  String merchantRegion;
  String merchantTaxRegistration;
  String merchantTaxRegistered;
}
