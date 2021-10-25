package com.worldpay.pms.bse.engine.transformations.model.input.mincharge;

import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import java.sql.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class MinimumChargeBillRow {
  @NonNull
  String billPartyId;
  @NonNull
  String legalCounterparty;
  @NonNull
  String currency;
  Date logicalDate;
  int partitionId;

  public static MinimumChargeBillRow from(BillRow bill) {
    return MinimumChargeBillRow.builder()
        .billPartyId(bill.getPartyId())
        .legalCounterparty(bill.getLegalCounterparty())
        .currency(bill.getCurrencyCode())
        .logicalDate(bill.getBillDate())
        .build();
  }
}
