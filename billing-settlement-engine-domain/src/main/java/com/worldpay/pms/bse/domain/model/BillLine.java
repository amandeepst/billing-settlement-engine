package com.worldpay.pms.bse.domain.model;

import java.math.BigDecimal;

public interface BillLine {

  String getBillLineId();

  String getBillLineParty();

  String getProductClass();

  String getProductId();

  String getProductDescription();

  String getPricingCurrency();

  String getFundingCurrency();

  BigDecimal getFundingAmount();

  String getTransactionCurrency();

  BigDecimal getTransactionAmount();

  BigDecimal getQuantity();

  String getPriceLineId();

  BigDecimal getTotalAmount();

  String getTaxStatus();

  String getPreviousBillLine();

  BillLineCalculation[] getBillLineCalculations();
}
