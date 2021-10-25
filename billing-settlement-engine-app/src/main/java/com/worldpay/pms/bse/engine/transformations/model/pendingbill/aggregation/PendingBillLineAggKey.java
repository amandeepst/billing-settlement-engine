package com.worldpay.pms.bse.engine.transformations.model.pendingbill.aggregation;

import lombok.NonNull;
import lombok.Value;

/**
 * Aggregation Fields:
 * <ul>
 *   <li>BILL_LINE_PARTY_ID
 *   <li>PRODUCT_ID
 *   <li>PRICE_CCY
 *   <li>FUND_CCY
 *   <li>TXN_CCY
 *   <li>PRICE_LN_ID
 *   <li>MERCHANT_CODE
 * </ul>
 */
@Value
public class PendingBillLineAggKey {

  @NonNull
  String billLinePartyId;
  @NonNull
  String productIdentifier;
  String pricingCurrency;
  String fundingCurrency;
  String transactionCurrency;
  String priceLineId;
  String merchantCode;

}
