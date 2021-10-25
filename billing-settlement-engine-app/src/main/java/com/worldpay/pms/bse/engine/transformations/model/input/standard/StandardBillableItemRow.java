package com.worldpay.pms.bse.engine.transformations.model.input.standard;

import java.sql.Date;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StandardBillableItemRow {

  private String billableItemId;

  private String subAccountId;

  private String legalCounterparty;

  private Date accruedDate;

  private String priceAssignId;
  private String settlementLevelType;
  private String settlementGranularity;

  private String billingCurrency;
  private String currencyFromScheme;
  private String fundingCurrency;
  private String priceCurrency;

  private String transactionCurrency;

  private String settlementLevelGranularity;

  private String productClass;
  private String merchantAmountSignage;
  private String merchantCode;
  private String childProduct;
  private String aggregationHash;
  private int partitionId;

  private Date ilmDate;
  private String priceItemCode;

  private Date firstFailureOn;
  private short retryCount;
}
