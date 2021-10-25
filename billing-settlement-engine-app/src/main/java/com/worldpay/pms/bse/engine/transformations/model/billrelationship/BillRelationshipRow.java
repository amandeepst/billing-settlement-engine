package com.worldpay.pms.bse.engine.transformations.model.billrelationship;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class BillRelationshipRow {
  @NonNull
  String parentBillId;
  @NonNull
  String childBillId;
  @NonNull
  String eventId;
  @NonNull
  String relationshipTypeId;
  @NonNull
  String paidInvoice;
  @NonNull
  String reuseDueDate;
  @NonNull
  String type;
  @NonNull
  String reasonCd;
}