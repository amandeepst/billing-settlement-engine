package com.worldpay.pms.bse.engine;

import com.worldpay.pms.bse.engine.transformations.model.IdGenerator;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class InMemoryBillIdGenerator implements IdGenerator {


  AtomicLong currentValue = new AtomicLong(1000000000000L);

  @Override
  public String generateId() {
    return String.valueOf(currentValue.incrementAndGet());
  }
}
