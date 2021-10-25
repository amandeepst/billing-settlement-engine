package com.worldpay.pms.bse.engine.transformations.model.pendingbill.aggregation;

import java.sql.Date;
import lombok.NonNull;
import lombok.Value;

/**
 * Aggregation Fields:
 * <ul>
 *   <li>ACCT_ID
 *   <li>END_DT
 *   <li>ADHOC_BILL
 *   <li>SETT_SUB_LVL_TYPE
 *   <li>SETT_SUB_LVL_VAL
 *   <li>GRANULARITY_KEY_VAL
 *   <li>DEBT_DT
 *   <li>DEBT_MIG_TYPE
 *   <li>OVERPAYMENT_FLG
 *   <li>REL_WAF_FLG
 *   <li>REL_RESERVE_FLG
 *   <li>FASTEST_PAY_ROUTE
 *   <li>CASE_ID
 *   <li>INDIVIDUAL_BILL
 *   <li>MANUAL_NARRATIVE
 * </ul>
 */
@Value
public class PendingBillAggKey {

  @NonNull
  String accountId;
  @NonNull
  Date scheduleEnd;
  @NonNull
  String adhocBillIndicator;
  String settlementSubLevelType;
  String settlementSubLevelValue;
  String granularityKeyValue;
  Date debtDate;
  String debtMigrationType;
  @NonNull
  String overpaymentIndicator;
  @NonNull
  String releaseWAFIndicator;
  @NonNull
  String releaseReserveIndicator;
  @NonNull
  String fastestPaymentRouteIndicator;
  String caseIdentifier;
  @NonNull
  String individualBillIndicator;
  @NonNull
  String manualBillNarrative;

}
