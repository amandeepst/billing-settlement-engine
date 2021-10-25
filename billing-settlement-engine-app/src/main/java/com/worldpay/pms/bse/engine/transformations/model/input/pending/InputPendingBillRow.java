package com.worldpay.pms.bse.engine.transformations.model.input.pending;

import java.sql.Date;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class InputPendingBillRow {

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

  private long partitionId;
}
