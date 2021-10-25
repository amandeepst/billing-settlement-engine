package com.worldpay.pms.bse.engine.transformations.model.input.correction;

import java.math.BigDecimal;
import java.sql.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.With;

@Data
@AllArgsConstructor
@NoArgsConstructor
@With
@Builder(toBuilder = true)
public class InputBillCorrectionRow {

  @NonNull
  private String billId;
  @NonNull
  private String correctionEventId;
  private String paidInvoice;
  private String type;
  private String reasonCd;

  private String partyId;
  private String billSubAccountId;
  private String tariffType;
  private String templateType;
  private String legalCounterparty;
  private String accountType;
  private String accountId;
  private String businessUnit;
  private Date billDate;
  private String billCycleId;
  private Date startDate;
  private Date endDate;
  private String currencyCode;
  private BigDecimal billAmount;
  private String billReference;
  private String status;
  private String adhocBillFlag;
  private String settlementSubLevelType;
  private String settlementSubLevelValue;
  private String granularity;
  private String granularityKeyValue;
  private String releaseWafIndicator;
  private String releaseReserveIndicator;
  private String fastestPaymentRouteIndicator;
  private String caseId;
  private String individualBillIndicator;
  private String manualNarrative;
  private String processingGroup;
  private String previousBillId;
  private Date debtDate;
  private String debtMigrationType;

  private String merchantTaxRegistrationNumber;
  private String worldpayTaxRegistrationNumber;
  private String taxType;
  private String taxAuthority;

  private long partitionId;
}
