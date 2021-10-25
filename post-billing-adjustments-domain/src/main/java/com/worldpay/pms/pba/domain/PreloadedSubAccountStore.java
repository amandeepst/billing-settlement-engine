package com.worldpay.pms.pba.domain;

import io.vavr.Function1;
import io.vavr.Tuple2;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import io.vavr.control.Try;
import lombok.Builder;
import lombok.NonNull;

public class PreloadedSubAccountStore extends LazyStore<String, Option<String>> implements SubAccountStore {

  @Builder
  public PreloadedSubAccountStore(@NonNull Integer maximumSize, @NonNull Integer concurrencyLevel,
      @NonNull Integer printStatsInterval, @NonNull Function1 loader) {
    super(maximumSize, concurrencyLevel, printStatsInterval, loader);
  }

  @Override
  public Option<String> getWafSubAccountId(String accountId) {
    return  get(accountId);
  }

  public void preload(Map<String, Stream<String>> saIds) {
    super.preload(saIds.map((id, sa) ->
            new Tuple2<>(id, sa.singleOption()))
        .mapValues(Try::success)
        .toJavaMap()
    );
  }

}
