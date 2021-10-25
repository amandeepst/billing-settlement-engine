package com.worldpay.pms.bse.domain.tax;

import static com.worldpay.pms.utils.Strings.isNullOrEmptyOrWhitespace;

import io.vavr.collection.List;
import java.util.Comparator;
import java.util.function.Function;

/**
 * Compares 2 objects based on the number of String fields on them that are null / empty / whitespace only.
 * The object which has less null fields from the provided list will be {@literal <} the other one.
 *
 * @param <T> Type of the object for which the comparison is done
 */
public class NullStringFieldsCountComparator<T> implements Comparator<T> {

  private final List<Function<T, String>> fieldGetters;

  public NullStringFieldsCountComparator(List<Function<T, String>> fieldGetters) {
    this.fieldGetters = fieldGetters;
  }

  @Override
  public int compare(T value1, T value2) {
    return Comparator.comparingInt(this::getNumberOfNullEmptyOrWhitespaceFields).compare(value1, value2);
  }

  private int getNumberOfNullEmptyOrWhitespaceFields(T value) {
    return fieldGetters
        .map(getter -> isNullOrEmptyOrWhitespace(getter.apply(value)) ? 1 : 0)
        .sum().intValue();
  }
}
