package com.worldpay.pms.pba.domain;

import com.worldpay.pms.pba.domain.model.WithholdFunds;
import io.vavr.Function1;
import io.vavr.Tuple2;

import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import io.vavr.control.Try;
import lombok.Builder;
import lombok.NonNull;


public class PreloadedWithholdFundsStore extends LazyStore<String, Option<WithholdFunds>>  implements
    WithholdFundsStore {

  @Builder
  public PreloadedWithholdFundsStore(@NonNull Integer maximumSize,
      @NonNull Integer concurrencyLevel, @NonNull Integer printStatsInterval,
      @NonNull Function1<String, Option<WithholdFunds>> loader) {
    super(maximumSize, concurrencyLevel, printStatsInterval, loader);
  }

  @Override
  public Option<WithholdFunds> getWithholdFundForAccount(String accountId) {
    return get(accountId);
  }

  public void preload(Map<String, Stream<WithholdFunds>> withholdFunds) {
    super.preload(withholdFunds
            .map((id, withholdFunds1) ->
                new Tuple2<>(id, withholdFunds1.singleOption()))
            .mapValues(Try::success)
            .toJavaMap()
    );
  }
}
