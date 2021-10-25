package com.worldpay.pms.bse.domain.model.mincharge;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.With;

@Data
@AllArgsConstructor
@NoArgsConstructor
@With
public class PendingMinimumCharge implements Serializable {

  @NonNull
  String billPartyId;
  @NonNull
  String legalCounterpartyId;
  String txnPartyId;
  LocalDate minChargeStartDate;
  LocalDate minChargeEndDate;
  String minChargeType;
  BigDecimal applicableCharges;
  LocalDate billDate;
  String currency;

}
