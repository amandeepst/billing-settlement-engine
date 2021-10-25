package com.worldpay.pms.bse.domain.model;

import java.sql.Date;

public interface BillableItem {

  String MISC_BILLABLE_ITEM = "MISC_BILLABLE_ITEM";
  String STANDARD_BILLABLE_ITEM = "STANDARD_BILLABLE_ITEM";

  String getBillableItemId();

  String getSubAccountId();

  String getLegalCounterparty();

  Date getAccruedDate();

  String getPriceLineId();

  String getSettlementLevelType();

  String getGranularityKeyValue();

  String getBillingCurrency();

  String getCurrencyFromScheme();

  String getFundingCurrency();

  String getPriceCurrency();

  String getTransactionCurrency();

  String getGranularity();

  String getAdhocBillFlag();

  String getBillableItemType();

  String getReleaseWAFIndicator();

  String getReleaseReserveIndicator();

  String getFastestSettlementIndicator();

  String getCaseIdentifier();

  String getIndividualPaymentIndicator();

  String getPaymentNarrative();

  String getProductClass();

  String getProductId();

  String getOverpaymentIndicator();

  String getMerchantCode();

  String getSourceType();

  String getSourceId();

  String getAggregationHash();

  Date getIlmDate();

  BillableItemLine[] getBillableItemLines();

  BillableItemServiceQuantity getBillableItemServiceQuantity();

  default boolean isMiscBillableItem() {
    return MISC_BILLABLE_ITEM.equals(getBillableItemType());
  }

  default boolean isStandardBillableItem() {
    return STANDARD_BILLABLE_ITEM.equals(getBillableItemType());
  }
}
