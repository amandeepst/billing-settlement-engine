package com.worldpay.pms.pba.engine;

import com.beust.jcommander.Parameter;
import com.typesafe.config.Config;
import com.worldpay.pms.cli.PmsParameterDelegate;
import com.worldpay.pms.cli.converter.LogicalDateConverter;
import com.worldpay.pms.config.ApplicationConfiguration;
import com.worldpay.pms.pba.engine.data.PostBillingBatchHistoryRepository;
import com.worldpay.pms.pba.engine.data.PostBillingRepository;
import com.worldpay.pms.pba.domain.WafProcessingService;
import com.worldpay.pms.pba.engine.transformations.PostBillingRepositoryFactory;
import com.worldpay.pms.pba.engine.transformations.WafProcessingServiceFactory;
import com.worldpay.pms.pba.engine.transformations.sources.BillSource;
import com.worldpay.pms.pba.engine.transformations.writers.PostBillAdjustmentAccountingWriter;
import com.worldpay.pms.pba.engine.transformations.writers.PostBillAdjustmentWriter;
import com.worldpay.pms.spark.core.JavaDriver;
import com.worldpay.pms.spark.core.PMSException;
import com.worldpay.pms.spark.core.SparkApp;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.batch.BatchHistoryRepository;
import com.worldpay.pms.spark.core.factory.ExecutorLocalFactory;
import com.worldpay.pms.spark.core.factory.Factory;
import io.vavr.Function1;
import io.vavr.Lazy;
import io.vavr.control.Option;
import java.time.LocalDate;
import java.util.function.Function;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class PostBillingEngine extends SparkApp<PostBillingConfiguration, PostBillingBatchRunResult> {

  final Function<BatchId, Long> generateRunId = Function1.of(this::getRunId).memoized();
  private Option<Long> runId = Option.none();

  private final Lazy<PostBillingBatchHistoryRepository> batchHistory;
  @Getter
  protected LocalDate logicalDate;

  public PostBillingEngine(Config config) {
    super(config, PostBillingConfiguration.class);
    this.batchHistory = Lazy.of(this::buildBatchHistoryRepository);
  }

  public static void main(String... args) {
    SparkApp.run(PostBillingEngine::new, args);
  }

  @Override
  public PmsParameterDelegate getParameterDelegate() {
    return new PostBillingParameterDelegate();
  }

  @Override
  public void parseExtraArgs(PmsParameterDelegate delegate) {
    if (delegate instanceof PostBillingParameterDelegate) {
      PostBillingParameterDelegate pbaDelegate = (PostBillingParameterDelegate) delegate;
      logicalDate = pbaDelegate.logicalDate;
    } else {
      throw new PMSException("Parameter delegate corrupted. Impossible state.");
    }
  }

  @Override
  protected BatchHistoryRepository buildBatchHistoryRepository(ApplicationConfiguration applicationConfiguration,
      PostBillingConfiguration postBillingConfiguration) {
    return batchHistory.get();
  }

  @Override
  protected JavaDriver<PostBillingBatchRunResult> buildDriver(SparkSession sparkSession,
      PostBillingConfiguration postBillingConfiguration) {
    log.info("Running with Configuration: \n{}", postBillingConfiguration);

    Factory<PostBillingRepository> repositoryFactory = new PostBillingRepositoryFactory(
        postBillingConfiguration.getSources().getWithholdFunds().getDataSource(), logicalDate);

    Factory<WafProcessingService> processingServiceFactory = ExecutorLocalFactory.of(
        new WafProcessingServiceFactory(repositoryFactory, postBillingConfiguration.getStore()));

    return new Driver(sparkSession,
        generateRunId,
        new BillSource(postBillingConfiguration.getSources().getBill()),
        new PostBillAdjustmentWriter(postBillingConfiguration.getWriters().getPostBillAdjustment()),
        new PostBillAdjustmentAccountingWriter(postBillingConfiguration.getWriters().getPostBillAdjustmentAccounting()),
        processingServiceFactory);

  }

  protected PostBillingBatchHistoryRepository buildBatchHistoryRepository() {
    return new PostBillingBatchHistoryRepository(getConf().getDb(), logicalDate, getSettings().getPublishers());
  }

  private Long getRunId(BatchId batchId) {
    return runId.getOrElse(() -> batchHistory.get().generateRunId(batchId));
  }


  private static class PostBillingParameterDelegate implements PmsParameterDelegate {

    @Parameter(
        names = {"--logical-date"}, arity = 1,
        description = "Date when bills were completed",
        required = true,
        converter = LogicalDateConverter.class)
    private LocalDate logicalDate;
  }
}
