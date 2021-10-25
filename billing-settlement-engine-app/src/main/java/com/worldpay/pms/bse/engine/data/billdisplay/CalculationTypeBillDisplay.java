package com.worldpay.pms.bse.engine.data.billdisplay;

import lombok.NonNull;
import lombok.Value;

@Value
public class CalculationTypeBillDisplay {

  @NonNull
  String calculationLineType;
  @NonNull
  String includeOnBillOutput;

}
