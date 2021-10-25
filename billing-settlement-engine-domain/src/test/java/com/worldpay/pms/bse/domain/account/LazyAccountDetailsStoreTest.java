package com.worldpay.pms.bse.domain.account;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.cache.CacheStats;
import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountDetails;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.Function1;
import io.vavr.collection.Stream;
import io.vavr.control.Validation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class LazyAccountDetailsStoreTest {

  @Test
  @DisplayName("When preload record then add record to cache")
  void preloadAddsEntriesToCache() {
    LazyAccountDetailsStore store = build(key -> DomainError.of("", "").asValidation());
    store.preload(Stream.of(new AccountDetails("ac1", "ROW", "WPMO")));

    Validation<DomainError, AccountDetails> result = store.get("ac1");
    store.get("ac2");
    CacheStats cacheStats = store.stats();

    assertThat(cacheStats.hitCount(), is(1L));
    assertThat(result.isValid(), is(true));
  }

  @Test
  @DisplayName("When loader function throws an error then the error is passed to the caller")
  void whenLoaderThrowsExceptionThenExceptionCascadesToCaller() {
    LazyAccountDetailsStore store = build(key -> {
      throw new RuntimeException("exception");
    });

    RuntimeException x = assertThrows(RuntimeException.class, () -> store.get(("ac1")));

    assertThat(x.getMessage(), is("exception"));
  }

  @Test
  @DisplayName("When create store with null fields then throw exception")
  void whenCreatingStoreWithNullParamsThenExceptionIsThrown() {
    assertThrows(NullPointerException.class, () -> LazyAccountDetailsStore.builder().build());
  }

  static LazyAccountDetailsStore build(Function1<String, Validation<DomainError, AccountDetails>> loader) {

    return LazyAccountDetailsStore.builder()
        .concurrencyLevel(1)
        .printStatsInterval(2)
        .maximumSize(2)
        .loader(loader)
        .build();
  }

}