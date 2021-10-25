package com.worldpay.pms.pba.engine.model.input;

import com.worldpay.pms.pba.domain.model.Bill;
import java.math.BigDecimal;
import java.sql.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class BillRow implements Bill {

  @NonNull
  private String billId;
  @NonNull
  private String billNumber;
  @NonNull
  private String partyId;
  @NonNull
  private String accountId;
  @NonNull
  private String billSubAccountId;
  private String tariffType;
  private String templateType;
  @NonNull
  private String legalCounterparty;
  @NonNull
  private String accountType;
  private String subAccountType;
  @NonNull
  private String businessUnit;
  @NonNull
  private Date billDate;
  @NonNull
  private String billCycleId;
  @NonNull
  private Date startDate;
  @NonNull
  private Date endDate;
  @NonNull
  private String currencyCode;
  @NonNull
  private BigDecimal billAmount;
  @NonNull
  private String billReference;
  @NonNull
  private String status;
  @NonNull
  private String adhocBillFlag;
  private String settlementSubLevelType;
  private String settlementSubLevelValue;
  private String granularity;
  private String granularityKeyValue;
  @NonNull
  private String releaseWafIndicator;
  @NonNull
  private String releaseReserveIndicator;
  @NonNull
  private String fastestPaymentRouteIndicator;
  private String caseId;
  @NonNull
  private String individualBillIndicator;
  @NonNull
  private String manualNarrative;
  private String processingGroup;
  private String previousBillId;
  private Date debtDate;
  private String debtMigrationType;

  private long partitionId;
}
