package com.worldpay.pms.bse.engine.transformations.model.pendingbill.aggregation;

import lombok.NonNull;
import lombok.Value;

/**
 * Aggregation Fields:
 * <ul>
 *   <li>CALC_LN_CLASS
 *   <li>CALC_LN_TYPE
 *   <li>INCLUDE_ON_BILL
 *   <li>RATE_TYPE
 * </ul>
 */
@Value
public class PendingBillLineCalculationAggKey {

  @NonNull
  String calculationLineClassification;
  String calculationLineType;
  @NonNull
  String includeOnBill;
  String rateType;

}
