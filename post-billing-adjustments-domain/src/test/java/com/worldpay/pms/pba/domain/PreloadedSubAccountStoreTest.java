package com.worldpay.pms.pba.domain;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.cache.CacheStats;
import io.vavr.Function1;
import io.vavr.Tuple2;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PreloadedSubAccountStoreTest {

  @Test
  @DisplayName("When preload record then add record to cache")
  void preloadAddsEntriesToCache() {
    PreloadedSubAccountStore store = build(key -> Option.none());
    store.preload(Stream.of(new Tuple2<>("ac1", "sa1")).toMap(Function.identity()).mapValues(Stream::of));

    Option<String> result = store.get("ac1");
    CacheStats cacheStats = store.stats();

    assertThat(cacheStats.hitCount(), is(1L));
    assertThat(result.isEmpty(), is(false));
  }

  @Test
  @DisplayName("When loader function throws an error then the error is passed to the caller")
  void whenLoaderThrowsExceptionThenExceptionCascadesToCaller() {
    PreloadedSubAccountStore store = build(key -> {
      throw new RuntimeException("exception");
    });

    RuntimeException x = assertThrows(RuntimeException.class, () -> store.get(("ac1")));

    assertThat(x.getMessage(), is("exception"));
  }

  @Test
  @DisplayName("When create store with null fields then throw exception")
  void whenCreatingStoreWithNullParamsThenExceptionIsThrown() {
    assertThrows(NullPointerException.class, () -> PreloadedSubAccountStore.builder().build());
  }


  static PreloadedSubAccountStore build(Function1<String, Option<String>> loader) {

    return PreloadedSubAccountStore.builder()
        .concurrencyLevel(1)
        .printStatsInterval(2)
        .maximumSize(2)
        .loader(loader)
        .build();
  }

}