package com.worldpay.pms.bse.engine.transformations.aggregation;

import static com.google.common.collect.Iterables.toArray;
import static java.math.BigDecimal.ZERO;

import com.google.common.collect.Maps;
import java.math.BigDecimal;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
public class PendingBillAggregationUtils {

  public static <K, V> V[] aggregateByKey(V[] array,
      Class<V> classTag,
      IntFunction<V[]> arrayFactory,
      Function<V, K> getKey,
      BiFunction<V, V, V> reducer) {
    // Aggregate the elements of an array based on a key function and a reduce function.

    if (array == null || array.length == 0) {
      return arrayFactory.apply(0);
    }

    Map<K, V> result = Maps.newHashMapWithExpectedSize(array.length);
    for (V item : array) {
      result.compute(getKey.apply(item), (k, v) -> v == null ? item : reducer.apply(v, item));
    }

    return toArray(result.values(), classTag);
  }

  public static <K, V> V[] aggregateByKey(V[] x, V[] y,
      Class<V> classTag,
      IntFunction<V[]> arrayFactory,
      Function<V, K> getKey,
      BiFunction<V, V, V> reducer) {
    // Aggregate the elements of 2 arrays based on a key function and a reduce function.
    // Assume the elements in each array are already aggregated.

    if (x == null || x.length == 0) {
      return (y == null) ? arrayFactory.apply(0) : y;
    }
    if (y == null || y.length == 0) {
      return x;
    }

    Map<K, V> result = Maps.newHashMapWithExpectedSize(Math.max(x.length, y.length));
    for (V item : x) {
      result.put(getKey.apply(item), item);
    }
    for (V item : y) {
      result.compute(getKey.apply(item), (k, v) -> v == null ? item : reducer.apply(v, item));
    }

    return toArray(result.values(), classTag);
  }

  public static <T, R> void validateAtMostOneIsNonNull(T x, T y, Function<T, R> fieldGetter, Supplier<RuntimeException> exceptionSupplier) {
    R xFieldValue = fieldGetter.apply(x);
    R yFieldValue = fieldGetter.apply(y);

    if (xFieldValue != null && yFieldValue != null) {
      throw exceptionSupplier.get();
    }
  }

  public static <T, R> R getFieldValue(T x, Function<T, R> fieldGetter) {
    return fieldGetter.apply(x);
  }

  public static <T, R> R getFieldValueOfNonNullId(T x, T y, Function<T, String> idGetter, Function<T, R> fieldGetter) {
    if (idGetter.apply(x) != null) {
      return fieldGetter.apply(x);
    }
    if (idGetter.apply(y) != null) {
      return fieldGetter.apply(y);
    }
    return fieldGetter.apply(x);
  }

  public static <T> T getNonNullId(T x, T y, Function<T, String> idGetter) {
    if (idGetter.apply(x) != null) {
      return x;
    }
    if (idGetter.apply(y) != null) {
      return y;
    }
    return x;
  }

  public static <T, R> R getFieldValueOfNullId(T x, T y, Function<T, String> idGetter, Function<T, R> fieldGetter) {
    if (idGetter.apply(x) == null) {
      return fieldGetter.apply(x);
    }
    if (idGetter.apply(y) == null) {
      return fieldGetter.apply(y);
    }
    return fieldGetter.apply(x);
  }

  public static <T> BigDecimal addNullableAmounts(T x, T y, Function<T, BigDecimal> amountGetter) {
    BigDecimal xAmount = getOrDefaultIfNull(amountGetter.apply(x), () -> ZERO);
    BigDecimal yAmount = getOrDefaultIfNull(amountGetter.apply(y), () -> ZERO);
    return xAmount.add(yAmount);
  }

  public static <T> String getFieldValueOfAnyIfTrue(T x, T y, Function<T, String> fieldGetter) {

    return "Y".equals(fieldGetter.apply(x)) || "Y".equals(fieldGetter.apply(y)) ? "Y" : "N";
  }

  public static <T> T getOrDefaultIfNull(T value, Supplier<T> defaultValueSupplier) {
    if (value == null) {
      return defaultValueSupplier.get();
    }
    return value;
  }

  public static void warnAfterThreshold(long valueToCheck, int threshold, String message, Object... messageArgs) {
    if (valueToCheck >= threshold) {
      log.warn(message, messageArgs);
    }
  }
}
