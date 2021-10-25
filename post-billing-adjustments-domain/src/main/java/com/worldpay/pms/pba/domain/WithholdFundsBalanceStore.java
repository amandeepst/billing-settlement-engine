package com.worldpay.pms.pba.domain;

import com.worldpay.pms.pba.domain.model.WithholdFundsBalance.WithholdFundsBalanceKey;
import io.vavr.control.Option;
import java.math.BigDecimal;

public interface WithholdFundsBalanceStore {

  Option<BigDecimal> get(WithholdFundsBalanceKey key);
}