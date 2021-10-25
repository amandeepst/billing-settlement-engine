package com.worldpay.pms.bse.engine.transformations.model.input.correction;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class InputBillLineCorrectionRow {

  @NonNull
  private String billId;
  private String billLineId;
  private String billLineParty;
  private String productClass;
  private String productId;
  private String productDescription;
  private String pricingCurrency;
  private String fundingCurrency;
  private BigDecimal fundingAmount;
  private String transactionCurrency;
  private BigDecimal transactionAmount;
  private BigDecimal quantity;
  private String priceLineId;
  private String merchantCode;
  private BigDecimal totalAmount;
  private String taxStatus;
  private String previousBillLine;
  private long partitionId;
}
