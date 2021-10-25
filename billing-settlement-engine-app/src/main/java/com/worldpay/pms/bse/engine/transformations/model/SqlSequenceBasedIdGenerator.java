package com.worldpay.pms.bse.engine.transformations.model;

import com.worldpay.pms.spark.core.jdbc.SqlDb;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SqlSequenceBasedIdGenerator implements IdGenerator{

  private final String statement;
  private final String sequenceName;
  private final int threshold;
  private final AtomicLong max;
  private final AtomicLong currentValue;
  private final SqlDb db;

  public SqlSequenceBasedIdGenerator(String sequenceName, int sequenceIncrementBy, SqlDb db) {
    statement = String.format("SELECT %s.nextVal FROM dual", sequenceName);
    this.sequenceName = sequenceName;
    threshold = sequenceIncrementBy;
    this.db = db;
    // initialize at the maximum so we are forced to get a value on the first invokation
    currentValue = new AtomicLong(0);
    max = new AtomicLong(0);
  }

  public long next() {
    long next = currentValue.incrementAndGet();
    if (next < max.get())
      return next;

    // cycle
    next = fetchNextValue();
    log.info("Retrieved value `{}` from sequence `{}`", next, sequenceName);
    currentValue.set(next);
    max.set(next + threshold);
    return next;
  }

  private long fetchNextValue() {
    return db.execQuery("get-next-seq-val", statement, q -> q.executeScalar(Long.class));
  }

  @Override
  public String generateId() {
    return String.valueOf(next());
  }
}
