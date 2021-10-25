package com.worldpay.pms.bse.engine.transformations.model.input.billtax;

import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTaxDetail;
import java.util.Collections;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class InputBillTaxRow {

  String billTaxId;
  @NonNull
  String billId;
  String merchantTaxRegistrationNumber;
  @NonNull
  String worldpayTaxRegistrationNumber;
  String taxType;
  String taxAuthority;
  String reverseChargeFlag;
  long partitionId;

  public BillTax toBillTax(BillTaxDetail[] billTaxDetails) {
    return new BillTax(billTaxId, billId, merchantTaxRegistrationNumber, worldpayTaxRegistrationNumber, taxType, taxAuthority,
        reverseChargeFlag, Collections.emptyMap(), billTaxDetails);
  }
}
