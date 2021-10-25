package com.worldpay.pms.bse.domain.model;

import java.math.BigDecimal;
import java.sql.Date;

public interface Bill {

  String getBillId();

  String getPartyId();

  String getBillSubAccountId();

  String getTariffType();

  String getTemplateType();

  String getLegalCounterparty();

  String getAccountType();

  String getBusinessUnit();

  Date getBillDate();

  String getBillCycleId();

  Date getStartDate();

  Date getEndDate();

  String getCurrencyCode();

  BigDecimal getBillAmount();

  String getBillReference();

  String getStatus();

  String getAdhocBillFlag();

  String getSettlementSubLevelType();

  String getSettlementSubLevelValue();

  String getGranularity();

  String getGranularityKeyValue();

  String getReleaseWafIndicator();

  String getReleaseReserveIndicator();

  String getFastestPaymentRouteIndicator();

  String getCaseId();

  String getIndividualBillIndicator();

  String getManualNarrative();

  String getProcessingGroup();

  Date getDebtDate();

  String getDebtMigrationType();

  BillLine[] getBillLines();

}
