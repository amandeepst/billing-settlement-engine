package com.worldpay.pms.pba.engine.transformations.writers;

import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.sql2o.Connection;
import org.sql2o.Query;

@Slf4j
class SequenceBasedIdGenerator {

  private final String statement;
  private final String sequenceName;
  private final int threshold;
  private final AtomicLong max;
  private final AtomicLong currentValue;

  public SequenceBasedIdGenerator(String sequenceName, int sequenceIncrementBy) {
    statement = String.format("SELECT %s.nextVal FROM dual", sequenceName);
    this.sequenceName = sequenceName;
    threshold = sequenceIncrementBy;
    // initialize at the maximum so we are forced to get a value on the first invokation
    currentValue = new AtomicLong(0);
    max = new AtomicLong(0);
  }

  public long next(Connection conn) {
    long next = currentValue.incrementAndGet();
    if (next < max.get())
      return next;

    // cycle
    next = fetchNextValue(conn);
    log.info("Retrieved value `{}` from sequence `{}`", next, sequenceName);
    currentValue.set(next);
    max.set(next + threshold);
    return next;
  }

  private long fetchNextValue(Connection conn) {
    try (Query q = conn.createQuery(statement)){
      return q.executeScalar(Long.class);
    }
  }
}
