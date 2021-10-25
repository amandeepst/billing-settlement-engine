package com.worldpay.pms.bse.engine.transformations.aggregation;

import static com.worldpay.pms.bse.engine.transformations.aggregation.PendingBillAggregationUtils.aggregateByKey;
import static com.worldpay.pms.bse.engine.transformations.aggregation.PendingBillAggregationUtils.warnAfterThreshold;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import lombok.Value;
import org.junit.jupiter.api.Test;

class PendingBillAggregationUtilsTest {

  @Test
  void whenArrayIsNullThenAggregationReturnsEmptyArray() {
    ValueHolder[] aggregated = aggregateByKey(null,
        ValueHolder.class, ValueHolder[]::new, ValueHolder::getKey, PendingBillAggregationUtilsTest::reduce);
    assertThat(aggregated.length, is(0));
  }

  @Test
  void whenArrayIsEmptyThenAggregationReturnsEmptyArray() {
    ValueHolder[] aggregated = aggregateByKey(new ValueHolder[]{},
        ValueHolder.class, ValueHolder[]::new, ValueHolder::getKey, PendingBillAggregationUtilsTest::reduce);
    assertThat(aggregated.length, is(0));
  }

  @Test
  void whenAggregateNonEmptyArrayThenReturnArrayWithAggregatedValues() {
    ValueHolder[] array = new ValueHolder[]{value("key1", 10), value("key2", 11), value("key1", 13)};
    ValueHolder[] aggregated = aggregateByKey(array,
        ValueHolder.class, ValueHolder[]::new, ValueHolder::getKey, PendingBillAggregationUtilsTest::reduce);
    ValueHolder[] expected = new ValueHolder[]{value("key1", 23), value("key2", 11)};
    assertThat(Arrays.equals(aggregated, expected), is(true));
  }

  @Test
  void whenAggregateArrayWith1ElementThenReturnSameArray() {
    ValueHolder[] array = new ValueHolder[]{value("key1", 10)};
    ValueHolder[] aggregated = aggregateByKey(array,
        ValueHolder.class, ValueHolder[]::new, ValueHolder::getKey, PendingBillAggregationUtilsTest::reduce);
    assertThat(Arrays.equals(aggregated, array), is(true));
  }

  @Test
  void whenArraysAreNullThenAggregationReturnsEmptyArray() {
    ValueHolder[] aggregated = aggregateByKey(null, null,
        ValueHolder.class, ValueHolder[]::new, ValueHolder::getKey, PendingBillAggregationUtilsTest::reduce);
    assertThat(aggregated.length, is(0));
  }

  @Test
  void whenArraysAreEmptyThenAggregationReturnsEmptyArray() {
    ValueHolder[] aggregated = aggregateByKey(new ValueHolder[]{}, new ValueHolder[]{},
        ValueHolder.class, ValueHolder[]::new, ValueHolder::getKey, PendingBillAggregationUtilsTest::reduce);
    assertThat(aggregated.length, is(0));
  }

  @Test
  void whenXIsNullOrEmptyThenAggregationReturnsY() {
    ValueHolder[] y = new ValueHolder[]{value("key1", 10)};

    ValueHolder[] aggregated = aggregateByKey(null, y,
        ValueHolder.class, ValueHolder[]::new, ValueHolder::getKey, PendingBillAggregationUtilsTest::reduce);
    assertThat(Arrays.equals(aggregated, y), is(true));

    aggregated = aggregateByKey(new ValueHolder[]{}, y,
        ValueHolder.class, ValueHolder[]::new, ValueHolder::getKey, PendingBillAggregationUtilsTest::reduce);
    assertThat(Arrays.equals(aggregated, y), is(true));
  }

  @Test
  void whenYIsNullOrEmptyThenAggregationReturnsX() {
    ValueHolder[] x = new ValueHolder[]{value("key1", 10)};

    ValueHolder[] aggregated = aggregateByKey(x, null,
        ValueHolder.class, ValueHolder[]::new, ValueHolder::getKey, PendingBillAggregationUtilsTest::reduce);
    assertThat(Arrays.equals(aggregated, x), is(true));

    aggregated = aggregateByKey(x, new ValueHolder[]{},
        ValueHolder.class, ValueHolder[]::new, ValueHolder::getKey, PendingBillAggregationUtilsTest::reduce);
    assertThat(Arrays.equals(aggregated, x), is(true));
  }

  @Test
  void whenAggregate2NonEmptyArraysThenReturnArrayWithAggregatedValues() {
    ValueHolder[] x = new ValueHolder[]{value("key1", 10), value("key2", 11)};
    ValueHolder[] y = new ValueHolder[]{value("key2", 12), value("key3", 13)};
    ValueHolder[] aggregated = aggregateByKey(x, y,
        ValueHolder.class, ValueHolder[]::new, ValueHolder::getKey, PendingBillAggregationUtilsTest::reduce);
    ValueHolder[] expected = new ValueHolder[]{value("key1", 10), value("key2", 23), value("key3", 13)};
    assertThat(Arrays.equals(aggregated, expected), is(true));
  }

  @Test
  void whenXHasOverlappingKeysThenAggregationWillOverwriteValueOfFirstOverlappingKey() {
    ValueHolder[] x = new ValueHolder[]{value("key1", 10), value("key2", 11), value("key1", 14)};
    ValueHolder[] y = new ValueHolder[]{value("key2", 12), value("key3", 13)};
    ValueHolder[] aggregated = aggregateByKey(x, y,
        ValueHolder.class, ValueHolder[]::new, ValueHolder::getKey, PendingBillAggregationUtilsTest::reduce);
    ValueHolder[] expected = new ValueHolder[]{value("key1", 14), value("key2", 23), value("key3", 13)};
    assertThat(Arrays.equals(aggregated, expected), is(true));
  }

  @Test
  void testWarnAfterThresholdWithPeriod() {
    warnAfterThreshold(60, 50, "{} gets logged", 60);
    warnAfterThreshold(50, 50, "{} gets logged", 50);
    warnAfterThreshold(40, 50, "{} doesn't get logged", 40);
    assertThat("Cannot assert logs", true);
  }

  private static ValueHolder value(String key, double value) {
    return new ValueHolder(key, value);
  }

  private static ValueHolder reduce(ValueHolder x, ValueHolder y) {
    return value(x.getKey(), x.getValue() + y.getValue());
  }

  @Value
  public static class ValueHolder {
    String key;
    double value;
  }
}