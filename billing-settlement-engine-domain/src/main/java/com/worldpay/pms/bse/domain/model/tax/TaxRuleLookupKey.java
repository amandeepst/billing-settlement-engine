package com.worldpay.pms.bse.domain.model.tax;

import lombok.Value;

@Value
public class TaxRuleLookupKey {

  String legalCounterPartyId;
  String merchantRegion;
  String merchantTaxRegistered;

  public static TaxRuleLookupKey from(TaxRule taxRule) {
    return new TaxRuleLookupKey(taxRule.getLegalCounterPartyId(), taxRule.getMerchantRegion(), taxRule.getMerchantTaxRegistered());
  }
}
