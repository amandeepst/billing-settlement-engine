package com.worldpay.pms.bse.domain.staticdata;

import lombok.NonNull;
import lombok.Value;

@Value
public class CalculationLineTypeDescription {

  @NonNull
  String calculationLineType;
  @NonNull
  String description;
}
