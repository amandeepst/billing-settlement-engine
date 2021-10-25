package com.worldpay.pms.pba.engine.transformations.writers;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.config.ApplicationConfiguration;
import com.worldpay.pms.pba.engine.utils.WithDatabase;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.stereotypes.WithSpark;
import java.util.ArrayList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.spark_project.guava.collect.Sets;
import org.sql2o.Connection;

@WithSpark
class SequenceBasedIdGeneratorTest implements WithDatabase {
  private static final String SEQUENCE_NAME = "CBE_PCE_OWNER.SOURCE_KEY_SQ";
  private static final int SEQUENCE_INCREMENT_BY = 10000;
  private static final int GENERATED_NUMBERS = 53000;
  protected SqlDb db;

  @BeforeEach
  void setUp(ApplicationConfiguration conf) {
    this.db = SqlDb.simple(conf.getDb());
  }

  private static void fillQueue(Queue<Long> queue, StageIdGenerator stage) {
    for (int i = 0; i < GENERATED_NUMBERS; i++) {
      queue.add(stage.getNextSourceKey());
    }
  }

  @Test
  public void generatesUniqueValuesOnSingleStage() {
    Queue<Long> generated = new ConcurrentLinkedQueue<>();
    db.exec("Single Thread ", conn -> {
      StageIdGenerator stage = new StageIdGenerator(SEQUENCE_INCREMENT_BY, SEQUENCE_NAME, conn);
      fillQueue(generated, stage);
      return stage;
    });
    assertUniqueNumbersGenerated(generated);
  }

  @Test
  public void generatesUniqueValuesOnMultipleStages() throws InterruptedException {
    ArrayList<StageIdGenerator> stages = new ArrayList<>();
    Queue<Long> generated = new ConcurrentLinkedQueue<>();
    db.exec("First thread", conn ->
        stages.add(new StageIdGenerator(SEQUENCE_INCREMENT_BY, SEQUENCE_NAME, conn))
    );
    db.exec("Second Thread", conn ->
        stages.add(new StageIdGenerator(SEQUENCE_INCREMENT_BY, SEQUENCE_NAME, conn))
    );
    ExecutorService executor = Executors.newFixedThreadPool(2);
    stages.forEach(stage -> executor.execute(() -> fillQueue(generated, stage)));

    executor.awaitTermination(10, TimeUnit.SECONDS);

    assertUniqueNumbersGenerated(generated);
  }

  private void assertUniqueNumbersGenerated(Queue<Long> queue) {
    assertThat(queue, hasSize(greaterThan(0)));
    assertThat(queue.size() % GENERATED_NUMBERS, is(0));
    //check that the generated numbers are unique
    Set<Long> mySet = Sets.newHashSet(queue);
    assertThat(mySet.size(), is(queue.size()));
  }

  private static class StageIdGenerator {

    private final int sourceKeyIncrementBy;
    private final String sourceKeySequenceName;
    private transient SequenceBasedIdGenerator incrementalIdProvider;
    private Connection connection;

    public StageIdGenerator(int sourceKeyIncrementBy, String sourceKeySequenceName, Connection connection) {
      this.sourceKeyIncrementBy = sourceKeyIncrementBy;
      this.sourceKeySequenceName = sourceKeySequenceName;
      this.connection = connection;
    }

    protected long getNextSourceKey() {
      if (incrementalIdProvider == null) {
        incrementalIdProvider = new SequenceBasedIdGenerator(sourceKeySequenceName, sourceKeyIncrementBy);
      }

      return incrementalIdProvider.next(connection);
    }

  }

}