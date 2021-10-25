package com.worldpay.pms.bse.domain.model;

import java.math.BigDecimal;

public interface PendingBillLine {

  String getBillLineId();

  String getBillLinePartyId();

  String getProductClass();

  String getProductIdentifier();

  String getPricingCurrency();

  String getFundingCurrency();

  BigDecimal getFundingAmount();

  String getTransactionCurrency();

  BigDecimal getTransactionAmount();

  BigDecimal getQuantity();

  String getPriceLineId();

  String getMerchantCode();

  PendingBillLineCalculation[] getPendingBillLineCalculations();

  PendingBillLineServiceQuantity[] getPendingBillLineServiceQuantities();

}
