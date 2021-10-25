package com.worldpay.pms.pba.domain;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.cache.CacheStats;
import com.worldpay.pms.pba.domain.model.WithholdFunds;
import io.vavr.Function1;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PreloadedWithholdFundsStoreTest {

  @Test
  @DisplayName("When preload record then add record to cache")
  void preloadAddsEntriesToCache() {
    PreloadedWithholdFundsStore store = build(key -> Option.none());
    store.preload(Stream.of(new WithholdFunds("ac1", "WAF", null, null))
        .groupBy(WithholdFunds::getAccountId));

    Option<WithholdFunds> result = store.get("ac1");
    CacheStats cacheStats = store.stats();

    assertThat(cacheStats.hitCount(), is(1L));
    assertThat(result.isEmpty(), is(false));
  }

  static PreloadedWithholdFundsStore build(Function1<String, Option<WithholdFunds>> loader) {

    return PreloadedWithholdFundsStore.builder()
        .concurrencyLevel(1)
        .printStatsInterval(0)
        .maximumSize(2)
        .loader(loader)
        .build();
  }

}