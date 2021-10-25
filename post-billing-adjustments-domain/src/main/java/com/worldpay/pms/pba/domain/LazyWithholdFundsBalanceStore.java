package com.worldpay.pms.pba.domain;

import com.worldpay.pms.pba.domain.model.WithholdFundsBalance.WithholdFundsBalanceKey;
import io.vavr.Function1;
import io.vavr.control.Option;
import java.math.BigDecimal;
import lombok.NonNull;

public class LazyWithholdFundsBalanceStore extends LazyStore<WithholdFundsBalanceKey, Option<BigDecimal>>
    implements WithholdFundsBalanceStore {

  public LazyWithholdFundsBalanceStore(@NonNull Integer maximumSize, @NonNull Integer concurrencyLevel,
      @NonNull Integer printStatsInterval,
      @NonNull Function1<WithholdFundsBalanceKey, Option<BigDecimal>> loader) {
    super(maximumSize, concurrencyLevel, printStatsInterval, loader);
  }
}