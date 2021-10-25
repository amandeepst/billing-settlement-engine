package com.worldpay.pms.bse.domain.model.mincharge;

import lombok.Value;

@Value
public class MinimumChargeKey {

  String legalCounterPartyId;
  String partyId;
  String currency;

  public static MinimumChargeKey from(MinimumCharge minimumCharge) {
    return new MinimumChargeKey(minimumCharge.getLegalCounterparty(), minimumCharge.getPartyId(), minimumCharge.getCurrency());
  }

  public static MinimumChargeKey from(PendingMinimumCharge pendingMinimumCharge) {
    return new MinimumChargeKey(pendingMinimumCharge.getLegalCounterpartyId(), pendingMinimumCharge.getTxnPartyId(),
        pendingMinimumCharge.getCurrency());
  }
}
