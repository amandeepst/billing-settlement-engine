package com.worldpay.pms.bse.engine.runresult;

import static com.typesafe.config.ConfigFactory.parseResources;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import com.worldpay.pms.bse.engine.BillingBatchRunResult;
import com.worldpay.pms.bse.engine.BillingConfiguration;
import com.worldpay.pms.bse.engine.BillingConfiguration.FailedBillsThreshold;
import com.worldpay.pms.config.ApplicationConfiguration;
import com.worldpay.pms.config.PmsConfiguration;
import com.worldpay.pms.spark.core.JavaDriver;
import com.worldpay.pms.spark.core.PMSException;
import com.worldpay.pms.spark.core.SparkApp;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.batch.Batch.BatchStep;
import com.worldpay.pms.spark.core.batch.Batch.Watermark;
import com.worldpay.pms.spark.core.batch.BatchHistoryConfig;
import com.worldpay.pms.spark.core.batch.BatchHistoryRepository;
import com.worldpay.pms.spark.core.batch.BatchMetadata;
import com.worldpay.pms.spark.core.batch.BatchRunErrorPolicy;
import com.worldpay.pms.spark.core.batch.BatchRunErrorPolicy.CompositeBatchRunErrorPolicy;
import com.worldpay.pms.spark.core.batch.BatchRunErrorPolicy.PercentThreshold;
import java.time.LocalDateTime;
import java.util.Optional;
import lombok.Data;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

class AppThresholdTest {

  private static final Batch DUMMY_BATCH = Batch
      .builder()
      .createdAt(LocalDateTime.now())
      .id(new Batch.BatchId("id", new Watermark(LocalDateTime.now(), LocalDateTime.now()), 0))
      .comments("test")
      .step(BatchStep.STARTED)
      .build();

  @Test
  void testIgnoredEventsAreNotUsedForThreshold() {
    Config conf = parseResources("application-test.conf")
        .resolve()
        .withValue("settings.failed-bills-threshold.failed-bill-rows-threshold-percent",
            ConfigValueFactory.fromAnyRef(-1))
        .withValue("settings.failed-bills-threshold.failed-bill-rows-threshold-count",
            ConfigValueFactory.fromAnyRef(10));
    DummyExtraApp app = new DummyExtraApp(conf);
    assertDoesNotThrow(() -> app.run(DUMMY_BATCH));  }

  @Test
  void executionDoesNotFailWhenNumberOfExtraErrorsIsLessThanAbsoluteThreshold() {
    Config conf = parseResources("application-test.conf").resolve();
    DummyExtraApp app = new DummyExtraApp(conf);
    assertDoesNotThrow(() -> app.run(DUMMY_BATCH));  }

  @Test
  void executionFailsWhenNumberOfBillableItemErrorsGreaterThanAbsoluteThreshold() {
    Config conf = parseResources("application-test.conf")
        .resolve()
        .withValue("settings.failed-rows-threshold-count",
            ConfigValueFactory.fromAnyRef(4));
    DummyExtraApp app = new DummyExtraApp(conf);
    PMSException pmsException = assertThrows(PMSException.class, () -> app.run(DUMMY_BATCH),
        "The app should have thrown an exception for exceeding the threshold");
    assertEquals(
        "Wrong message thrown by the app for exceeding the threshold",
        "Driver interrupted, error 'billable items' exceed absolute threshold of '4', with '10' errors from '100' total",
        pmsException.getMessage()
    );
  }

  @Test
  void executionFailsWhenPercentOfBillableItemErrorsGreaterThanPercentThreshold() {
    Config conf = parseResources("application-test.conf")
        .resolve()
        .withValue("settings.failed-rows-threshold-percent",
            ConfigValueFactory.fromAnyRef(3));
    DummyExtraApp app = new DummyExtraApp(conf);
    PMSException pmsException = assertThrows(PMSException.class, () -> app.run(DUMMY_BATCH),
        "The app should have thrown an exception for exceeding the threshold");
    assertEquals(
        "Wrong message thrown by the app for exceeding the threshold",
        "Driver interrupted, error 'billable items' exceed percent threshold of '3.0%', with '10' errors from '100' total",
        pmsException.getMessage()
    );
  }

  @Test
  void executionFailsWhenNumberOfBillErrorsGreaterThanAbsoluteExtraThreshold() {
    Config conf = parseResources("application-test.conf")
        .resolve()
        .withValue("settings.failed-bills-threshold.failed-bill-rows-threshold-count",
                   ConfigValueFactory.fromAnyRef(4));
    DummyExtraApp app = new DummyExtraApp(conf);
    PMSException pmsException = assertThrows(PMSException.class, () -> app.run(DUMMY_BATCH),
                                             "The app should have thrown an exception for exceeding the threshold");
    assertEquals(
        "Wrong message thrown by the app for exceeding the threshold",
        "Driver interrupted, error 'bills' exceed absolute threshold of '4', with '9' errors from '100' total",
        pmsException.getMessage()
    );
  }

  @Test
  void executionFailsWhenPercentOfBillErrorsGreaterThanPercentExtraThreshold() {
    Config conf = parseResources("application-test.conf")
        .resolve()
        .withValue("settings.failed-bills-threshold.failed-bill-rows-threshold-percent",
                   ConfigValueFactory.fromAnyRef(3));
    DummyExtraApp app = new DummyExtraApp(conf);
    PMSException pmsException = assertThrows(PMSException.class, () -> app.run(DUMMY_BATCH),
                                             "The app should have thrown an exception for exceeding the threshold");
    assertEquals(
        "Wrong message thrown by the app for exceeding the threshold",
        "Driver interrupted, error 'bills' exceed percent threshold of '3.0%', with '9' errors from '100' total",
        pmsException.getMessage()
    );
  }

  static class DummyExtraApp extends SparkApp<DummyExtraPmsConfiguration, BillingBatchRunResult> {

    DummyExtraApp(Config config) {
      super(config, DummyExtraPmsConfiguration.class);
      init();
    }

    @Override
    protected BatchHistoryRepository buildBatchHistoryRepository(ApplicationConfiguration conf, DummyExtraPmsConfiguration settings) {
      return new DummyBatchHistoryRepository();
    }

    @Override
    public JavaDriver<BillingBatchRunResult> buildDriver(SparkSession spark, DummyExtraPmsConfiguration conf) {
      return new JavaDriver<BillingBatchRunResult>(spark) {
        @Override
        public BillingBatchRunResult run(Batch batch) {
          return BillingBatchRunResult
              .builder()
              .billableItemResultCount(new ResultCount(20, 30, 120, 10, 0))
              .billResultCount(new ResultCount(20, 29, 120, 9, 0))
              .build();
        }
      };
    }

    @Override
    protected BatchRunErrorPolicy getErrorPolicy() {
      FailedBillsThreshold failedBillsThreshold = getSettings().getFailedBillsThreshold();
      return new CompositeBatchRunErrorPolicy(ImmutableList.of(
          new BillingPercentThresholdPolicy(PercentThreshold.of(getSettings().getFailedRowsThresholdPercent()),
                                            BillingBatchRunResult.BILLABLE_ITEMS_RESULT_TYPE),
          new BillingAbsoluteThresholdPolicy(getSettings().getFailedRowsThresholdCount(),
                                             BillingBatchRunResult.BILLABLE_ITEMS_RESULT_TYPE),
          new BillingPercentThresholdPolicy(PercentThreshold.of(failedBillsThreshold.getFailedBillRowsThresholdPercent()),
                                            BillingBatchRunResult.BILLS_RESULT_TYPE),
          new BillingAbsoluteThresholdPolicy(getSettings().getFailedBillsThreshold().getFailedBillRowsThresholdCount(),
                                             BillingBatchRunResult.BILLS_RESULT_TYPE)
      ));
    }
  }

  @lombok.Data
  public static class DummyExtraPmsConfiguration implements PmsConfiguration {

    private int maxAttempts;
    @com.typesafe.config.Optional
    private BatchHistoryConfig history;
    private double failedRowsThresholdPercent;
    @com.typesafe.config.Optional
    private long failedRowsThresholdCount = Long.MAX_VALUE;

    @com.typesafe.config.Optional
    private BillingConfiguration.FailedBillsThreshold failedBillsThreshold;

    @Data
    public static class FailedBillsThreshold {
      @com.typesafe.config.Optional
      private double failedBillRowsThresholdPercent = -1.0D;
      @com.typesafe.config.Optional
      private long failedBillRowsThresholdCount = Long.MAX_VALUE;
    }
  }

  static class DummyBatchHistoryRepository implements BatchHistoryRepository {

    @Override
    public Optional<Batch> getLastSuccessfulBatch() {
      return Optional.empty();
    }

    @Override
    public ImmutableList<Batch> findOverlappingBatches(Watermark watermark) {
      return null;
    }

    @Override
    public Batch addBatch(BatchId id, BatchStep step, Optional<String> comment, Optional<BatchMetadata> metadata) {
      return Batch.builder().createdAt(LocalDateTime.now()).id(id).comments(comment.orElse("test")).step(step).build();
    }

    @Override
    public Optional<Batch> getBatchBy(String batchCode, Optional<Integer> attempt) {
      return Optional.of(DUMMY_BATCH);
    }

    @Override
    public ImmutableList<Batch> getRunningBatches() {
      return null;
    }
  }

}