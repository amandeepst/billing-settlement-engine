package com.worldpay.pms.bse.engine;

import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.worldpay.pms.bse.domain.BillProcessingService;
import com.worldpay.pms.bse.domain.BillableItemProcessingService;
import com.worldpay.pms.bse.domain.account.AccountDeterminationService;
import com.worldpay.pms.bse.domain.account.AccountRepository;
import com.worldpay.pms.bse.engine.BillingConfiguration.FailedBillsThreshold;
import com.worldpay.pms.bse.engine.data.AccountRepositoryFactory;
import com.worldpay.pms.bse.engine.data.BillingBatchHistoryRepository;
import com.worldpay.pms.bse.engine.data.BillingRepository;
import com.worldpay.pms.bse.engine.data.BillingRepositoryFactory;
import com.worldpay.pms.bse.engine.domain.billcorrection.BillCorrectionService;
import com.worldpay.pms.bse.engine.domain.billcorrection.BillCorrectionServiceFactory;
import com.worldpay.pms.bse.engine.runresult.BillingAbsoluteThresholdPolicy;
import com.worldpay.pms.bse.engine.runresult.BillingPercentThresholdPolicy;
import com.worldpay.pms.bse.engine.transformations.AccountDeterminationServiceFactory;
import com.worldpay.pms.bse.engine.transformations.BillProcessingServiceFactory;
import com.worldpay.pms.bse.engine.transformations.BillableItemProcessingServiceFactory;
import com.worldpay.pms.bse.engine.transformations.BillIdGeneratorFactory;
import com.worldpay.pms.bse.engine.transformations.model.IdGenerator;
import com.worldpay.pms.bse.engine.transformations.registry.DataRegistry;
import com.worldpay.pms.cli.PmsParameterDelegate;
import com.worldpay.pms.config.ApplicationConfiguration;
import com.worldpay.pms.spark.core.JavaDriver;
import com.worldpay.pms.spark.core.SparkApp;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.batch.BatchHistoryRepository;
import com.worldpay.pms.spark.core.batch.BatchRunErrorPolicy;
import com.worldpay.pms.spark.core.batch.BatchRunErrorPolicy.CompositeBatchRunErrorPolicy;
import com.worldpay.pms.spark.core.batch.BatchRunErrorPolicy.PercentThreshold;
import com.worldpay.pms.spark.core.factory.ExecutorLocalFactory;
import com.worldpay.pms.spark.core.factory.Factory;
import io.vavr.Function1;
import io.vavr.Lazy;
import io.vavr.control.Option;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class BillingEngine extends SparkApp<BillingConfiguration, BillingBatchRunResult> {

  private final Lazy<BillingBatchHistoryRepository> batchHistory;
  final Function<BatchId, Long> generateRunId = Function1.of(this::getRunId).memoized();
  private Option<Long> runId = Option.none();
  protected LocalDate logicalDate;
  protected boolean overrideBillableItemAccruedDate;
  protected List<String> processingGroups;
  protected List<String> billingTypes;

  public BillingEngine(Config config) {
    super(config, BillingConfiguration.class);
    batchHistory = Lazy.of(this::buildBatchHistory);
  }

  public static void main(String... args) {
    SparkApp.run(BillingEngine::new, args);
  }

  @Override
  public PmsParameterDelegate getParameterDelegate() {
    return new BillingEngineParameterDelegate();
  }

  @Override
  public void parseExtraArgs(PmsParameterDelegate delegate) {
    BillingEngineParameterDelegate pEDelegate = BillingEngineParameterDelegate.getParameters(delegate);
    runId = Option.of(pEDelegate.getRunId());
    logicalDate = pEDelegate.getLogicalDate();
    processingGroups = pEDelegate.getProcessingGroups();
    overrideBillableItemAccruedDate = pEDelegate.isOverrideBillableItemAccruedDate();
    billingTypes = pEDelegate.getBillingTypes();
  }

  @Override
  protected BatchHistoryRepository buildBatchHistoryRepository(ApplicationConfiguration conf, BillingConfiguration settings) {
    return batchHistory.get();
  }

  @Override
  protected JavaDriver<BillingBatchRunResult> buildDriver(SparkSession sparkSession, BillingConfiguration settings) {
    log.info("Running with Configuration: \n{}", settings);

    DataRegistry dataRegistry = new DataRegistry(settings, sparkSession,
        Option.of(Date.valueOf(logicalDate)).filter(x -> overrideBillableItemAccruedDate), logicalDate);

    Factory<AccountRepository> accountRepositoryFactory = new AccountRepositoryFactory(settings.getSources().getAccountData(),
        logicalDate);
    Factory<BillingRepository> billingRepositoryFactory = new BillingRepositoryFactory(settings.getSources().getStaticData(),
        Date.valueOf(logicalDate), accountRepositoryFactory);

    Factory<AccountDeterminationService> accountDeterminationServiceFactory = ExecutorLocalFactory.of(
        new AccountDeterminationServiceFactory(accountRepositoryFactory, settings.getBilling()));

    Factory<BillableItemProcessingService> billableItemProcessingServiceFactory =
        new BillableItemProcessingServiceFactory(accountDeterminationServiceFactory);
    Factory<BillProcessingService> billProcessingServiceFactory = new BillProcessingServiceFactory(accountDeterminationServiceFactory,
        billingRepositoryFactory, logicalDate, processingGroups, billingTypes, settings.getBilling());
    Factory<BillCorrectionService> billCorrectionServiceFactory = new BillCorrectionServiceFactory();
    Factory<IdGenerator> idGeneratorFactory = new BillIdGeneratorFactory(settings.getBillIdSeqConfiguration());

    return new Driver(
        sparkSession,
        generateRunId,
        dataRegistry.getReaderRegistry(),
        dataRegistry.getWriterRegistry(),
        billableItemProcessingServiceFactory,
        billProcessingServiceFactory,
        billingRepositoryFactory,
        settings.getDriverConfig(),
        billCorrectionServiceFactory,
        idGeneratorFactory);
  }

  private Long getRunId(BatchId batchId) {
    return runId.getOrElse(() -> batchHistory.get().generateRunId(batchId));
  }

  protected BillingBatchHistoryRepository buildBatchHistory() {
    return new BillingBatchHistoryRepository(getConf().getDb(), logicalDate, getSettings().getPublishers());
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
