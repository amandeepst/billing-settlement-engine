package com.worldpay.pms.bse.engine.transformations.model.completebill;

import io.vavr.collection.Stream;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BillLineDetailRow {

  @NonNull
  private String billId;
  @NonNull
  private String billLineId;
  @NonNull
  private String billItemId;
  @NonNull
  private String billItemHash;

  public static BillLineDetailRow[] from(String billId, String billLineId, BillLineDetail[] billItems) {
    return Stream.of(billItems)
        .map(entry -> new BillLineDetailRow(billId, billLineId, entry.getBillItemId(), entry.getBillItemHash()))
        .toJavaArray(BillLineDetailRow[]::new);
  }
}
