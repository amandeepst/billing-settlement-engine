package com.worldpay.pms.pba.domain.model;

import java.math.BigDecimal;
import lombok.Value;

@Value
public class WithholdFunds {

  String accountId;

  String withholdFundType;

  BigDecimal withholdFundTarget;

  Double withholdFundPercentage;

}
