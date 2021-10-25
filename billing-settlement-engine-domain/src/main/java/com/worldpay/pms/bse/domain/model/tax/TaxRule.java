package com.worldpay.pms.bse.domain.model.tax;

import lombok.NonNull;
import lombok.Value;

@Value
public class TaxRule {

  @NonNull
  String legalCounterPartyId;
  @NonNull
  String legalCounterPartyCountry;
  @NonNull
  String legalCounterPartyCurrency;
  String merchantCountry;
  @NonNull
  String merchantRegion;
  @NonNull
  String merchantTaxRegistered;
  @NonNull
  String taxAuthority;
  @NonNull
  String taxType;
  @NonNull
  String taxStatusType;
  @NonNull
  String reverseCharge;
  String legalCounterPartyTaxRegNumber;
}
