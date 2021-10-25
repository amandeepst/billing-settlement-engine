package com.worldpay.pms.pba.domain.model;

import java.math.BigDecimal;
import lombok.NonNull;
import lombok.Value;

@Value
public class Adjustment {

  @NonNull
  String type;
  String description;
  @NonNull
  BigDecimal amount;
  @NonNull
  String subAccountId;
  String subAccountType = "WAF";
}
