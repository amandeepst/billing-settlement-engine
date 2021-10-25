package com.worldpay.pms.bse.engine.transformations.model.input.mincharge;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class PendingMinChargeRowWithKey {

  String legalCounterpartyId;
  String partyId;
  String currency;
  PendingMinChargeRow[] pendingMinimumChargeRows;

}
