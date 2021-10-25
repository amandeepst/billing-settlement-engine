package com.worldpay.pms.bse.engine;

import com.worldpay.pms.spark.core.factory.Factory;
import io.vavr.control.Try;
import lombok.Value;

@Value
public class DummyFactory<T> implements Factory<T> {

  T value;

  @Override
  public Try<T> build() {
    return Try.success(value);
  }
}
