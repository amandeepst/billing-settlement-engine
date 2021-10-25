package com.worldpay.pms.bse.engine.transformations.model.input.billtax;

import com.worldpay.pms.bse.domain.model.billtax.BillTaxDetail;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class InputBillTaxDetailRow {

  String billTaxId;
  @NonNull
  String taxStatus;
  @NonNull
  BigDecimal taxRate;
  @NonNull
  String taxStatusDescription;
  @NonNull
  BigDecimal netAmount;
  @NonNull
  BigDecimal taxAmount;
  long partitionId;

  public BillTaxDetail toBillTaxDetail() {
    return new BillTaxDetail(billTaxId, taxStatus, taxRate, taxStatusDescription, netAmount, taxAmount);
  }
}
