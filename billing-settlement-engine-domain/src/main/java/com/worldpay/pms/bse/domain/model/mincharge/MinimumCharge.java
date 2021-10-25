package com.worldpay.pms.bse.domain.model.mincharge;

import com.worldpay.pms.bse.domain.account.BillingCycle;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
@AllArgsConstructor
public class MinimumCharge {

  String priceAssignId;
  String legalCounterparty;
  String partyId;
  String rateType;
  BigDecimal rate;
  String currency;

  BillingCycle cycle;
}
