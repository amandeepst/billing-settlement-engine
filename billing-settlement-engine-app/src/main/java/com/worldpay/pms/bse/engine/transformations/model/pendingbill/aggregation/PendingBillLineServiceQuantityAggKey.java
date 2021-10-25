package com.worldpay.pms.bse.engine.transformations.model.pendingbill.aggregation;

import lombok.NonNull;
import lombok.Value;

/**
 * Aggregation Fields:
 * <ul>
 *   <li>SVC_QTY_CD
 * </ul>
 */
@Value
public class PendingBillLineServiceQuantityAggKey {

  @NonNull
  String serviceQuantityTypeCode;

}
