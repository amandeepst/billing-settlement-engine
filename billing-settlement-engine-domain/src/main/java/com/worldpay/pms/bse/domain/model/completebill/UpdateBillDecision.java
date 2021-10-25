package com.worldpay.pms.bse.domain.model.completebill;

import java.time.LocalDate;
import lombok.NonNull;
import lombok.Value;

@Value
public class UpdateBillDecision {

  String billCycleId;
  LocalDate scheduleEnd;
  boolean shouldUpdate;

  public static UpdateBillDecision doNotUpdateDecision() {
    return new UpdateBillDecision(null, null, false);
  }

  public static UpdateBillDecision updateDecision(@NonNull String billCycleId, @NonNull LocalDate scheduleEnd) {
    return new UpdateBillDecision(billCycleId, scheduleEnd, true);
  }
}
