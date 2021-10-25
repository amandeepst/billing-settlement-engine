package com.worldpay.pms.bse.domain.account;

import java.time.LocalDate;
import lombok.NonNull;
import lombok.Value;

@Value
public class BillingCycle {

  @NonNull
  String code;

  LocalDate startDate;

  LocalDate endDate;
}
