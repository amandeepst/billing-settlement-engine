package com.worldpay.pms.pba.domain;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.vavr.Function1;
import io.vavr.control.Try;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LazyStore<K, V> {

  // guava cache holding a limited number of entries at once
  private final LoadingCache<K, Try<? extends V>> store;

  // internal counter to track how many requests we have received
  private final AtomicLong totalRequests;
  private final int printStatsInterval;

  public LazyStore(
      @NonNull Integer maximumSize, @NonNull Integer concurrencyLevel,
      @NonNull Integer printStatsInterval,
      @NonNull Function1<K, V> loader) {

    this.printStatsInterval = printStatsInterval;
    this.totalRequests = new AtomicLong();

    log.info(
      "Creating LazyStore {} with maximumSize={} and concurrencyLevel={}...", getClass().getSimpleName(), maximumSize, concurrencyLevel
    );

    this.store = CacheBuilder.newBuilder()
        // allow garbage collection to get rid of our values when it decides proper
        .softValues()
        // also tell the cache instance to get rid of objects after we go over certain threshold
        .maximumSize(maximumSize)
        // set to the same parallelism degree on the executors
        .concurrencyLevel(concurrencyLevel)
        // keep stats which we'll print every so often
        .recordStats()
        .removalListener(this::onRemoval)
        .build(new CacheLoader<K, Try<? extends V>>() {
          @Override
          public Try<? extends V> load(K key) {
            return Try.of(() -> loader.apply(key));
          }
        });
  }

  /** eagerly preload a set of entries */
  protected void preload(Map<K, Try<V>> values) {
    store.putAll(values);
  }

  @SneakyThrows
  public V get(K key) {
    try {
      return store.get(key).get();
    } catch (ExecutionException | UncheckedExecutionException e ) {
      log.error("Failure when fetching from lazy store for key {}", key, e);
      throw e.getCause();
    } finally {
      tryPrintStats();
    }
  }

  //
  // internal impl
  //

  public CacheStats stats() {
    return this.store.stats();
  }

  private void onRemoval(RemovalNotification<K, Try<? extends V>> notification) {
    log.info("Evicting key `{}` from store, cause: {}", notification.getKey(), notification.getCause());
  }

  private void tryPrintStats() {
    Try.run(() -> {
      long request = totalRequests.incrementAndGet();
      if (request % printStatsInterval == 0) {
        log.info("LazyPriceListStore stats: {}", this.stats());
      }
    }).onFailure(e -> log.warn("Failed printing stats.", e));

  }
}
