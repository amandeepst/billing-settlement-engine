package com.worldpay.pms.bse.domain.model;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class TestPendingBillLineServiceQuantity implements PendingBillLineServiceQuantity {

  private String serviceQuantityTypeCode;
  private BigDecimal serviceQuantityValue;
}
