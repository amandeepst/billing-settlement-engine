package com.worldpay.pms.bse.engine.transformations.model.input.mincharge;

import com.worldpay.pms.bse.domain.model.mincharge.PendingMinimumCharge;
import java.math.BigDecimal;
import java.sql.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.With;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
@With
public class PendingMinChargeRow {

  @NonNull
  String billPartyId;
  @NonNull
  String legalCounterparty;
  @NonNull
  String txnPartyId;
  Date minChargeStartDate;
  Date minChargeEndDate;
  @NonNull
  String minChargeType;
  @NonNull
  BigDecimal applicableCharges;
  Date billDate;
  String currency;
  @NonNull
  int partitionId;

  
  public static PendingMinChargeRow from(PendingMinimumCharge minimumCharge) {
    return new PendingMinChargeRow(
        minimumCharge.getBillPartyId(),
        minimumCharge.getLegalCounterpartyId(),
        minimumCharge.getTxnPartyId(),
        Date.valueOf(minimumCharge.getMinChargeStartDate()),
        Date.valueOf(minimumCharge.getMinChargeEndDate()),
        minimumCharge.getMinChargeType(),
        minimumCharge.getApplicableCharges(),
        Date.valueOf(minimumCharge.getBillDate()),
        minimumCharge.getCurrency(),
        0
    );
  }
}
