package com.worldpay.pms.bse.domain.mincharge;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.cache.CacheStats;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumCharge;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumChargeKey;
import io.vavr.collection.List;
import io.vavr.control.Option;
import java.math.BigDecimal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PreloadedMinimumChargeStoreTest {

  @Test
  @DisplayName("When preload record then add record to cache")
  void preloadAddsEntriesToCache() {
    PreloadedMinimumChargeStore store = build();
    store.preloadMinCharge(
        List.of(new MinimumCharge("345","P00001", "P00005", "MIN_CHG", BigDecimal.valueOf(15), "GBP",null),
            new MinimumCharge("123","P00001", "P00005", "MIN_CHG", BigDecimal.TEN, "GBP",null)));

    Option<MinimumCharge> result = store.get(new MinimumChargeKey("P00001","P00005", "GBP"));
    CacheStats cacheStats = store.stats();

    assertThat(cacheStats.hitCount(), is(1L));
    assertThat(result.isEmpty(), is(false));
    assertThat(result.get(), is(new MinimumCharge("123","P00001", "P00005", "MIN_CHG", BigDecimal.TEN, "GBP",null)));
  }

  static PreloadedMinimumChargeStore build() {
    return PreloadedMinimumChargeStore.builder()
        .concurrencyLevel(1)
        .printStatsInterval(2)
        .maximumSize(2)
        .loader(key -> Option.none())
        .build();
  }

}