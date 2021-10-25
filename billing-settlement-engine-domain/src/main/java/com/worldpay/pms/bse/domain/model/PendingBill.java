package com.worldpay.pms.bse.domain.model;

import static com.worldpay.pms.bse.domain.common.Utils.FLAG_YES;

import java.sql.Date;

public interface PendingBill {

  String getBillId();

  String getPartyId();

  String getLegalCounterpartyId();

  String getAccountId();

  String getBillSubAccountId();

  String getAccountType();

  String getBusinessUnit();

  String getBillCycleId();

  Date getScheduleStart();

  Date getScheduleEnd();

  String getCurrency();

  String getBillReference();

  String getAdhocBillIndicator();

  String getSettlementSubLevelType();

  String getSettlementSubLevelValue();

  String getGranularity();

  String getGranularityKeyValue();

  Date getDebtDate();

  String getDebtMigrationType();

  String getOverpaymentIndicator();

  String getReleaseWAFIndicator();

  String getReleaseReserveIndicator();

  String getFastestPaymentRouteIndicator();

  String getCaseIdentifier();

  String getIndividualBillIndicator();

  String getManualBillNarrative();

  String getMiscalculationFlag();

  PendingBillLine[] getPendingBillLines();

  String getBillableItemId();

  Date getBillableItemIlmDate();

  Date getBillableItemFirstFailureOn();

  int getBillableItemRetryCount();

  default boolean isAdhocBill() {
    return FLAG_YES.equalsIgnoreCase(getAdhocBillIndicator());
  }
}
