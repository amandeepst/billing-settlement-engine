package com.worldpay.pms.bse.engine.transformations.model.completebill;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BillLineDetail {

  @NonNull
  private String billItemId;
  @NonNull
  private String billItemHash;

  public static BillLineDetail from(String billItemId, String billItemHash) {
    return new BillLineDetail(billItemId, billItemHash);
  }

  public static BillLineDetail[] array(String billItemId, String billItemHash) {
    return new BillLineDetail[]{from(billItemId, billItemHash)};
  }
}
