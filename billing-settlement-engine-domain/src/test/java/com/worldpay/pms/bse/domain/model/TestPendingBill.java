package com.worldpay.pms.bse.domain.model;

import java.sql.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.With;

@Data
@AllArgsConstructor
@With
@Builder
public class TestPendingBill implements PendingBill {

  private String billId;
  private String partyId;
  private String legalCounterpartyId;
  private String accountId;
  private String billSubAccountId;
  private String accountType;
  private String businessUnit;
  private String billCycleId;
  private Date scheduleStart;
  private Date scheduleEnd;
  private String currency;
  private String billReference;
  private String adhocBillIndicator;
  private String settlementSubLevelType;
  private String settlementSubLevelValue;
  private String granularity;
  private String granularityKeyValue;
  private Date debtDate;
  private String debtMigrationType;
  private String overpaymentIndicator;
  private String releaseWAFIndicator;
  private String releaseReserveIndicator;
  private String fastestPaymentRouteIndicator;
  private String caseIdentifier;
  private String individualBillIndicator;
  private String manualBillNarrative;
  private String miscalculationFlag;
  private Date firstFailureOn;
  private short retryCount;
  private TestPendingBillLine[] pendingBillLines;
  private String billableItemId;
  private Date billableItemIlmDate;
  private Date billableItemFirstFailureOn;
  private int billableItemRetryCount;
}
