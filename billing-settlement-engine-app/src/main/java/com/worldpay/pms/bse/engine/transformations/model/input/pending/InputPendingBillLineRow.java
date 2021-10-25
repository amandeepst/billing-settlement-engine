package com.worldpay.pms.bse.engine.transformations.model.input.pending;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class InputPendingBillLineRow {

  private String billLineId;
  private String billId;
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
  private long partitionId;
}
