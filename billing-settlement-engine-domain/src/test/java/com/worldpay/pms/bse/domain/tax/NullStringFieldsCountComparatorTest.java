package com.worldpay.pms.bse.domain.tax;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import io.vavr.collection.List;
import java.util.function.Function;
import lombok.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NullStringFieldsCountComparatorTest {

  private NullStringFieldsCountComparator<TestValue> comparator;

  @BeforeEach
  void setUp() {
    comparator = new NullStringFieldsCountComparator<>(TestValue.GETTERS);
  }

  @Test
  void whenCompare2ValuesThenTheOneWithLessNullFieldsIsSmallerThenTheOneWithMoreFields() {
    TestValue value1 = testValue("one", "two");
    TestValue value2 = testValue("one", null);
    TestValue value3 = testValue(null, "two");
    TestValue value4 = testValue(null, null);

    assertThat(comparator.compare(value1, value2), equalTo(-1));
    assertThat(comparator.compare(value2, value1), equalTo(+1));
    assertThat(comparator.compare(value2, value3), equalTo(0));
    assertThat(comparator.compare(value3, value4), equalTo(-1));
  }

  @Test
  void whenCompare2ValuesThenTheOneWithLessEmptyFieldsIsSmallerThenTheOneWithMoreFields() {
    TestValue value1 = testValue("one", "two");
    TestValue value2 = testValue("one", "");

    assertThat(comparator.compare(value1, value2), equalTo(-1));
    assertThat(comparator.compare(value2, value1), equalTo(+1));
  }

  @Test
  void whenCompare2ValuesThenTheOneWithLessWhitespaceFieldsIsSmallerThenTheOneWithMoreFields() {
    TestValue value1 = testValue("one", "two");
    TestValue value2 = testValue("one", " ");

    assertThat(comparator.compare(value1, value2), equalTo(-1));
    assertThat(comparator.compare(value2, value1), equalTo(+1));
  }

  private static TestValue testValue(String one, String two) {
    return new TestValue(one, two);
  }

  @Value
  private static class TestValue {

    final static List<Function<TestValue, String>> GETTERS = List.of(
        TestValue::getOne,
        TestValue::getTwo
    );

    String one;
    String two;
  }
}