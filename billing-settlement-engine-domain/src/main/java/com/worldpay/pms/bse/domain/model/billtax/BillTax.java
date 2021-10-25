package com.worldpay.pms.bse.domain.model.billtax;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.With;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder(toBuilder = true)
public class BillTax {

  private String billTaxId;
  @With
  private String billId;
  private String merchantTaxRegistrationNumber;
  @NonNull
  private String worldpayTaxRegistrationNumber;
  private String taxType;
  private String taxAuthority;
  private String reverseChargeFlag;

  private Map<String, BillLineTax> billLineTaxes;
  private BillTaxDetail[] billTaxDetails;

  public String getBillLineTaxStatus(String billLineId) {
    return billLineTaxes.get(billLineId).getTaxStatus();
  }

  public BillLineTax getBillLineTax(String billLineId) {
    return billLineTaxes.get(billLineId);
  }

  public static BillLineTax reduceBillLineTaxes(BillLineTax left, BillLineTax right) {
    return new BillLineTax(
        left.getTaxStatus(),
        left.getTaxRate(),
        left.getTaxStatusDescription(),
        left.getNetAmount().add(right.getNetAmount()),
        left.getTaxAmount().add(right.getTaxAmount())
    );
  }

}
