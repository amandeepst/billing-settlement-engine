package com.worldpay.pms.bse.domain.model;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.With;

@Data
@AllArgsConstructor
@With
@Builder
public class TestPendingBillLine implements PendingBillLine {

  private String billLineId;
  private String billLinePartyId;
  private String productClass;
  private String productIdentifier;
  private String pricingCurrency;
  private String fundingCurrency;
  private BigDecimal fundingAmount;
  private String transactionCurrency;
  private BigDecimal transactionAmount;
  private BigDecimal quantity;
  private String priceLineId;
  private String merchantCode;
  private TestPendingBillLineCalculation[] pendingBillLineCalculations;
  private TestPendingBillLineServiceQuantity[] pendingBillLineServiceQuantities;

}
