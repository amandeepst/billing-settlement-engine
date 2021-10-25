package com.worldpay.pms.bse.engine.data;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import java.math.BigDecimal;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ObjectPool {

  private static final Interner<String> STRING_POOL = Interners.newWeakInterner();

  public String intern(String value) {
    if (value == null) {
      return null;
    }

    return STRING_POOL.intern(value);
  }

  public BigDecimal intern(BigDecimal value) {
    if (value == null) {
      return null;
    }

    if (value.compareTo(BigDecimal.ZERO) == 0) {
      return BigDecimal.ZERO;
    }
    if (value.compareTo(BigDecimal.ONE) == 0) {
      return BigDecimal.ONE;
    }

    return value;
  }
}