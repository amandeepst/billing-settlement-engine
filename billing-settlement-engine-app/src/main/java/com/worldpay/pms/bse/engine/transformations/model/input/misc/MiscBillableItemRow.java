package com.worldpay.pms.bse.engine.transformations.model.input.misc;

import java.math.BigDecimal;
import java.sql.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MiscBillableItemRow {

  private String billableItemId;

  private String subAccountId;

  private String legalCounterparty;

  private Date accruedDate;

  private String adhocBillFlag;

  private String currencyCode;

  private String productClass;

  private String productId;

  private BigDecimal quantity;

  private Date ilmDate;
  private int partitionId;

  private String releaseWAFIndicator;
  private String releaseReserveIndicator;
  private String fastestSettlementIndicator;
  private String caseIdentifier;
  private String individualPaymentIndicator;
  private String paymentNarrative;
  private Date debtDate;
  private String sourceType;
  private String sourceId;

  private Date firstFailureOn;
  private short retryCount;
}
