package com.worldpay.pms.bse.domain;

import java.math.BigDecimal;

@FunctionalInterface
public interface RoundingService {

  BigDecimal roundAmount(BigDecimal amount, String currency);

}
