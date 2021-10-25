package com.worldpay.pms.pba.domain.model;

import java.math.BigDecimal;
import lombok.Value;

@Value
public class WithholdFundsBalance {

  String partyId;
  String legalCounterparty;
  String currency;
  BigDecimal balance;

  @Value
  public static class WithholdFundsBalanceKey {

    String partyId;
    String legalCounterparty;
    String currency;
  }

  public static WithholdFundsBalance from(WithholdFundsBalanceKey key, BigDecimal balance) {
    return new WithholdFundsBalance(key.getPartyId(), key.getLegalCounterparty(), key.getCurrency(), balance);
  }
}