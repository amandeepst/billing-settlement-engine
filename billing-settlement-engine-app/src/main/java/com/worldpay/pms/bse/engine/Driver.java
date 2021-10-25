package com.worldpay.pms.bse.engine;

import static com.google.common.collect.Iterators.forArray;
import static com.google.common.collect.Iterators.toArray;
import static com.worldpay.pms.bse.engine.Encodings.PENDING_BILL_MINIMUM_CHARGE_TUPLE_ENCODER;
import static com.worldpay.pms.bse.engine.transformations.BillProcessing.completePendingBills;
import static com.worldpay.pms.bse.engine.transformations.BillProcessing.getBillLineDetails;
import static com.worldpay.pms.bse.engine.transformations.TransformationUtils.enrichDatasetWithUUIds;

import com.worldpay.pms.bse.domain.BillProcessingService;
import com.worldpay.pms.bse.domain.BillableItemProcessingService;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumChargeKey;
import com.worldpay.pms.bse.engine.BillingConfiguration.DriverConfig;
import com.worldpay.pms.bse.engine.data.BillingRepository;
import com.worldpay.pms.bse.engine.domain.billcorrection.BillCorrectionService;
import com.worldpay.pms.bse.engine.runresult.ResultCount;
import com.worldpay.pms.bse.engine.transformations.BillCorrecting;
import com.worldpay.pms.bse.engine.transformations.BillProcessing;
import com.worldpay.pms.bse.engine.transformations.CompleteBillResult;
import com.worldpay.pms.bse.engine.transformations.PendingBillProcessing;
import com.worldpay.pms.bse.engine.transformations.PendingBillResult;
import com.worldpay.pms.bse.engine.transformations.UpdateBillResult;
import com.worldpay.pms.bse.engine.transformations.aggregation.PendingBillAggregation;
import com.worldpay.pms.bse.engine.transformations.model.IdGenerator;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemRow;
import com.worldpay.pms.bse.engine.transformations.model.billaccounting.BillAccountingRow;
import com.worldpay.pms.bse.engine.transformations.model.billerror.BillError;
import com.worldpay.pms.bse.engine.transformations.model.billprice.BillPriceRow;
import com.worldpay.pms.bse.engine.transformations.model.billrelationship.BillRelationshipRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetailRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import com.worldpay.pms.bse.engine.transformations.model.failedbillableitem.FailedBillableItem;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillCorrectionRow;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.MinimumChargeBillRow;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.PendingMinChargeRow;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.PendingMinChargeRowWithKey;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import com.worldpay.pms.bse.engine.transformations.registry.ReaderRegistry;
import com.worldpay.pms.bse.engine.transformations.registry.WriterRegistry;
import com.worldpay.pms.spark.core.DataSource;
import com.worldpay.pms.spark.core.DataWriter;
import com.worldpay.pms.spark.core.ErrorWriter;
import com.worldpay.pms.spark.core.JavaDriver;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.factory.ExecutorLocalFactory;
import com.worldpay.pms.spark.core.factory.Factory;
import io.vavr.collection.Stream;
import java.util.function.Function;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

@Slf4j
public class
Driver extends JavaDriver<BillingBatchRunResult> {

  private final Function<BatchId, Long> runIdGenerator;

  //sources
  private final DataSource<BillableItemRow> standardBillableItemSource;
  private final DataSource<BillableItemRow> miscBillableItemSource;
  private final DataSource<PendingBillRow> pendingBillSource;
  private final DataSource<PendingMinChargeRow> pendingMinChargeSource;
  private final DataSource<Tuple2<InputBillCorrectionRow, BillLineRow[]>> billCorrectionSource;
  private final DataSource<BillTax> billTaxDataSource;

  //writers
  private final DataWriter<PendingBillRow> pendingBillWriter;
  private final DataWriter<BillRow> completeBillWriter;
  private final DataWriter<BillLineDetailRow> billLineDetailWriter;
  private final ErrorWriter<BillError> billErrorWriter;
  private final DataWriter<BillError> fixedBillErrorWriter;
  private final ErrorWriter<FailedBillableItem> failedBillableItemWriter;
  private final DataWriter<FailedBillableItem> completedPrevFailedBillableWriter;
  private final DataWriter<Tuple2<String, BillPriceRow>> billPriceWriter;
  private final DataWriter<BillTax> billTaxWriter;
  private final DataWriter<PendingMinChargeRow> pendingMinChargeWriter;
  private final DataWriter<BillAccountingRow> billAccountingWriter;
  private final DataWriter<BillRelationshipRow> billRelationshipWriter;
  private final DataWriter<MinimumChargeBillRow> minimumChargeBillWriter;

  private final Factory<BillableItemProcessingService> billableItemProcessingServiceFactory;
  private final Factory<BillProcessingService> billProcessingServiceFactory;
  private final Factory<BillingRepository> repositoryFactory;
  private final Factory<BillCorrectionService> billCorrectionServiceFactory;
  private final Factory<IdGenerator> billingIdGeneratorFactory;

  private final DriverConfig driverConfig;

  public Driver(SparkSession spark,
      Function<BatchId, Long> runIdProvider,
      ReaderRegistry readerRegistry,
      WriterRegistry writerRegistry,
      Factory<BillableItemProcessingService> billableItemProcessingServiceFactory,
      Factory<BillProcessingService> billProcessingServiceFactory,
      Factory<BillingRepository> repositoryFactory, DriverConfig driverConfig,
      Factory<BillCorrectionService> billCorrectionServiceFactory,
      Factory<IdGenerator> billingIdGeneratorFactory) {
    super(spark);
    this.runIdGenerator = runIdProvider;
    this.standardBillableItemSource = readerRegistry.getStandardBillableItemSource();
    this.miscBillableItemSource = readerRegistry.getMiscBillableItemSource();
    this.pendingBillSource = readerRegistry.getPendingBillSource();
    this.pendingMinChargeSource = readerRegistry.getPendingMinChargeSource();
    this.billCorrectionSource = readerRegistry.getBillCorrectionSource();
    this.billTaxDataSource = readerRegistry.getBillTaxDataSource();
    this.pendingBillWriter = writerRegistry.getPendingBillWriter();
    this.completeBillWriter = writerRegistry.getCompleteBillWriter();
    this.billLineDetailWriter = writerRegistry.getBillLineDetailWriter();
    this.billErrorWriter = writerRegistry.getBillErrorWriter();
    this.fixedBillErrorWriter = writerRegistry.getFixedBillErrorWriter();
    this.failedBillableItemWriter = writerRegistry.getFailedBillableItemWriter();
    this.completedPrevFailedBillableWriter = writerRegistry.getFixedBillableWriter();
    this.billPriceWriter = writerRegistry.getBillPriceWriter();
    this.billTaxWriter = writerRegistry.getBillTaxWriter();
    this.pendingMinChargeWriter = writerRegistry.getPendingMinChargeWriter();
    this.billAccountingWriter = writerRegistry.getBillAccountingWriter();
    this.billRelationshipWriter = writerRegistry.getBillRelationshipWriter();
    this.minimumChargeBillWriter = writerRegistry.getMinimumChargeBillWriter();

    this.billableItemProcessingServiceFactory = billableItemProcessingServiceFactory;
    this.billProcessingServiceFactory = billProcessingServiceFactory;
    this.repositoryFactory = repositoryFactory;

    this.driverConfig = driverConfig;
    this.billCorrectionServiceFactory = billCorrectionServiceFactory;
    this.billingIdGeneratorFactory = billingIdGeneratorFactory;
  }

  public BillingBatchRunResult run(Batch batch) {

    log.info(
        "Running batch={} attempt={} from={} to={} ilm_dt={}",
        batch.id.code,
        batch.id.attempt,
        batch.id.watermark.low,
        batch.id.watermark.high,
        batch.createdAt
    );

    long runId = runIdGenerator.apply(batch.id);
    log.info("Generated runId `{}` for batch `{}`", runId, batch.id);

    // broadcast the factory but not the data, we want to load the config from the db directly on the executors
    // due to it's size
    val repositoryFactoryBroadcast = ExecutorLocalFactory.of(jsc, repositoryFactory);
    val billableItemServiceFactoryBroadcast = ExecutorLocalFactory.of(jsc, billableItemProcessingServiceFactory);
    val billProcessingServiceFactoryBroadcast = ExecutorLocalFactory.of(jsc, billProcessingServiceFactory);
    val billCorrectionServiceFactoryBroadcast = ExecutorLocalFactory.of(jsc, billCorrectionServiceFactory);
    val idGeneratorFactoryBroadcast = ExecutorLocalFactory.of(jsc, billingIdGeneratorFactory);

    val billableItems = readBillableItems(batch)
        .persist(StorageLevel.MEMORY_AND_DISK_SER());
    long billableItemCount = namedAction("Read Input Billable Items", jsc -> billableItems.count());
    log.info("Returned billableItemCount={} billable items", billableItemCount);

    val newPendingBills = PendingBillProcessing.toPendingBillResult(
        billableItems, billableItemServiceFactoryBroadcast, repositoryFactoryBroadcast)
        .persist(StorageLevel.MEMORY_AND_DISK_SER());

    long newPendingBillResultCount = namedAction("Compute New Pending Bills", jsc -> newPendingBills.count());
    log.info("Returned newPendingBillResultCount={} new pending bill results", newPendingBillResultCount);

    val successNewPendingBills = newPendingBills
        .filter(PendingBillResult::isSuccess)
        .map(PendingBillResult::getSuccess, Encodings.PENDING_BILL_ENCODER);
    val failedBillableItems = newPendingBills
        .filter(PendingBillResult::isFailure)
        .map(PendingBillResult::getFailure, Encodings.FAILED_BILLABLE_ITEM_ENCODER)
        .alias("Failed Billable Items");
    val fixedBillableItems = newPendingBills
        .filter(PendingBillResult::isPreviouslyFailedAndFixed)
        .map(pendingBillResult -> FailedBillableItem.fixed(pendingBillResult.getSuccess()), Encodings.FAILED_BILLABLE_ITEM_ENCODER)
        .alias("Fixed Billable Items");

    val inputPendingBills = BillProcessing.updatePendingBills(
        pendingBillSource.load(spark, batch.id), billProcessingServiceFactoryBroadcast)
        .persist(StorageLevel.MEMORY_AND_DISK_SER());
    long dbPendingBillResultCount = namedAction("Compute Db Pending Bills", jsc -> inputPendingBills.count());
    log.info("Returned dbPendingBillResultCount={} db pending bill results", dbPendingBillResultCount);

    val successInputPendingBills = inputPendingBills
        .filter(UpdateBillResult::isSuccess)
        .map(UpdateBillResult::getPendingBill, Encodings.PENDING_BILL_ENCODER);
    val errorInputPendingBills = inputPendingBills
        .filter(UpdateBillResult::isError)
        .map(UpdateBillResult::getPendingBill, Encodings.PENDING_BILL_ENCODER);
    val billErrorsForInputPendingBills = inputPendingBills
        .filter(UpdateBillResult::isError)
        .map(UpdateBillResult::getBillError, Encodings.BILL_ERROR_ENCODER);

    val pendingBills = successNewPendingBills.union(successInputPendingBills);
    val pendingBillsBeforeCompletion = PendingBillProcessing.generateIds(PendingBillAggregation.aggregate(pendingBills),
        idGeneratorFactoryBroadcast);

    val inputPendingMinCharges = pendingMinChargeSource.load(spark, batch.id)
        .groupByKey(pending ->
                new MinimumChargeKey(pending.getLegalCounterparty(), pending.getBillPartyId(), pending.getCurrency()),
            Encodings.MINIMUM_CHARGE_KEY_ENCODER)
        .mapGroups((key, group) ->
                new PendingMinChargeRowWithKey(key.getLegalCounterPartyId(), key.getPartyId(), key.getCurrency(),
                    toArray(group, PendingMinChargeRow.class)),
            Encodings.PENDING_MINIMUM_CHARGE_TUPLE_ENCODER);

    val pendingDataJoin = pendingBillsBeforeCompletion
        .joinWith(inputPendingMinCharges,
            pendingBillsBeforeCompletion.col("legalCounterpartyId")
                .equalTo(inputPendingMinCharges.col("legalCounterpartyId"))
                .and(pendingBillsBeforeCompletion.col("partyId")
                    .equalTo(inputPendingMinCharges.col("partyId")))
                .and(pendingBillsBeforeCompletion.col("currency")
                    .equalTo(inputPendingMinCharges.col("currency")))
                .and(pendingBillsBeforeCompletion.col("accountType").equalTo("CHRG"))
                .and(pendingBillsBeforeCompletion.col("adhocBillIndicator").equalTo("N"))
                .and(pendingBillsBeforeCompletion.col("overpaymentIndicator").equalTo("N"))
                .and(pendingBillsBeforeCompletion.col("individualBillIndicator").equalTo("N"))
                .and(pendingBillsBeforeCompletion.col("caseIdentifier").equalTo("N")),
            "full_outer")
        .map(tuple -> new Tuple2<>(tuple._1(), tuple._2() == null ? null : tuple._2().getPendingMinimumChargeRows()),
            PENDING_BILL_MINIMUM_CHARGE_TUPLE_ENCODER);

    val billCorrections = billCorrectionSource.load(spark, batch.id);
    val billTaxCorrections = billTaxDataSource.load(spark, batch.id);

    val correctedBills = BillCorrecting.computeBillCorrection(billCorrections, billTaxCorrections, billCorrectionServiceFactoryBroadcast,
        idGeneratorFactoryBroadcast)
        .persist(StorageLevel.MEMORY_AND_DISK_SER());
    long correctedBillsCount = namedAction("Running Credit Note ",jsc -> correctedBills.count());
    log.info("Returned correctedBillsCount={} corrected bills", correctedBillsCount);

    val correctedBillRelationship = BillCorrecting.computeBillRelationship(correctedBills, billCorrectionServiceFactoryBroadcast);

    // handle complete previously failed transactions
    val completeBillResults =
        completePendingBills(pendingDataJoin, billProcessingServiceFactoryBroadcast,
            repositoryFactoryBroadcast, idGeneratorFactoryBroadcast)
            .filter(CompleteBillResult::isNotEmpty)
            .union(correctedBills)
            .persist(StorageLevel.fromString(driverConfig.getCompleteBillStorageLevel()));

    long completeBillResultCount = namedAction("Running Bill Completion", jsc -> completeBillResults.count());
    log.info("Returned completeBillResultCount={} complete bill results", completeBillResultCount);

    val successAndErrorPendingBills = completeBillResults
        .filter(CompleteBillResult::isPending)
        .map(CompleteBillResult::getPendingBill, Encodings.PENDING_BILL_ENCODER)
        .union(errorInputPendingBills);
    val successCompleteBillResult = completeBillResults
        .filter(CompleteBillResult::isComplete);
    val successCompleteBillsWithTax = successCompleteBillResult
        .filter(billResult -> billResult.getCompleteBill().getBillTax() != null);
    val billTaxes = successCompleteBillsWithTax
        .map(billResult -> billResult.getCompleteBill().getBillTax(), Encodings.BILL_TAX);
    val billAccountingFromTax = successCompleteBillsWithTax
        .flatMap(billResult -> forArray(BillAccountingRow.from(billResult.getCompleteBill(), billResult.getCompleteBill().getBillTax())),
            Encodings.BILL_ACCOUNTING_ROW_ENCODER);
    val successCompleteBills = successCompleteBillResult
        .map(CompleteBillResult::getCompleteBill, Encodings.BILL_ROW_ENCODER);
    val billAccounting = successCompleteBills
        .flatMap(bill -> forArray(BillAccountingRow.from(bill)), Encodings.BILL_ACCOUNTING_ROW_ENCODER)
        .union(billAccountingFromTax);
    val billLineDetails = getBillLineDetails(completeBillResults);
    val pendingMinimumCharges = completeBillResults
        .filter(CompleteBillResult::isPendingMinimumCharges)
        .flatMap(result -> forArray(result.getPendingMinimumCharges()), Encodings.PENDING_MINIMUM_CHARGE_ENCODER)
        .map(PendingMinChargeRow::from, Encodings.PENDING_MINIMUM_CHARGE_ROW_ENCODER);
    val minChargeBill = successCompleteBills.filter(BillRow::isStandardChargingBill)
        .map(MinimumChargeBillRow::from, Encodings.MINIMUM_CHARGE_BILL_ROW_ENCODER);

    // Bill Price
    val miscBillPrices = completeBillResults
        .filter(billResult -> billResult.isPending() && !billResult.isError())
        .flatMap(x -> forArray(x.getPendingBill().getBillPrices()), Encodings.BILL_PRICE_ROW_ENCODER)
        .union(
            completeBillResults
                .filter(CompleteBillResult::isComplete)
                .flatMap(x -> forArray(x.getCompleteBill().getBillPrices()), Encodings.BILL_PRICE_ROW_ENCODER)
        );
    val taxBillPrices = successCompleteBillsWithTax
        .map(result -> BillPriceRow.from(Tuple2.apply(result.getCompleteBill(), result.getCompleteBill().getBillTax())),
            Encodings.BILL_PRICE_ROW_ENCODER);
    val minChargeBillPrices = successCompleteBills
        .filter(bill -> Stream.of(bill.getBillLines()).filter(line -> "MINCHRGP".equals(line.getProductId())).size() > 0)
        .flatMap(bill -> forArray(BillPriceRow.from(bill)), Encodings.BILL_PRICE_ROW_ENCODER);

    val billPrices = miscBillPrices.union(taxBillPrices).union(minChargeBillPrices);

    val billErrors = billErrorsForInputPendingBills.union(
        completeBillResults
            .filter(CompleteBillResult::isError)
            .map(CompleteBillResult::getBillError, Encodings.BILL_ERROR_ENCODER)
    );
    val fixedBillError = completeBillResults
        .filter(completeBillResult -> completeBillResult.isComplete() && completeBillResult.getCompleteBill().getFirstFailureOn() != null)
        .map(completeBillResult -> BillError.fixed(completeBillResult.getCompleteBill()), Encodings.BILL_ERROR_ENCODER);

    return outputAllBillingResults(batch, runId, billableItemCount,
        BillProcessingDatasets
            .builder()
            .failedBillableItems(failedBillableItems)
            .fixedBillableItems(fixedBillableItems)
            .completeBillResults(completeBillResults)
            .successAndErrorPendingBills(successAndErrorPendingBills)
            .successCompleteBills(successCompleteBills)
            .billTaxes(billTaxes)
            .billLineDetails(billLineDetails)
            .pendingMinimumCharges(pendingMinimumCharges)
            .minimumChargeBills(minChargeBill)
            .billPrices(billPrices)
            .billErrors(billErrors)
            .fixedBillError(fixedBillError)
            .billAccounting(billAccounting)
            .relationshipBillResults(correctedBillRelationship)
            .build());
  }

  private BillingBatchRunResult outputAllBillingResults(Batch batch, long runId, long billableItemCount,
      BillProcessingDatasets billProcessingDatasets) {
    long billableItemErrorCount = writeFailedBillableItems(billProcessingDatasets.failedBillableItems, batch);
    long previouslyFailedBillableItemsCount = writeCompletePreviouslyFailedBillableItems(billProcessingDatasets.fixedBillableItems, batch);
    long billableItemErrorFailedTodayCount = failedBillableItemWriter.getFirstFailureCount();
    long billableItemErrorIgnoredCount = failedBillableItemWriter.getIgnoredCount();

    long pendingBillCount = writePendingBills(billProcessingDatasets.successAndErrorPendingBills, batch);
    long billErrorCount = writeBillErrors(billProcessingDatasets.billErrors, batch);
    long billErrorFailedTodayCount = billErrorWriter.getFirstFailureCount();
    long billErrorIgnoredCount = billErrorWriter.getIgnoredCount();
    long billLineDetailCount = writeBillLineDetails(billProcessingDatasets.billLineDetails, batch);
    long fixedBillErrorCount = writeFixedBillErrors(billProcessingDatasets.fixedBillError, batch);
    long correctedBillsCount = writeCorrectedBillRelationship(billProcessingDatasets.relationshipBillResults, batch);

    return BillingBatchRunResult.builder()
        .runId(runId)
        .pendingBillCount(pendingBillCount)
        .billLineDetailCount(billLineDetailCount)
        .billPriceCount(writeBillPrices(enrichDatasetWithUUIds(billProcessingDatasets.billPrices, Encodings.BILL_PRICE_ROW_ENCODER), batch))
        .billResultCount(new ResultCount(
            billErrorIgnoredCount,
            billErrorCount,
            writeCompleteBills(billProcessingDatasets.successCompleteBills, batch) + billErrorCount,
            billErrorFailedTodayCount,
            fixedBillErrorCount
        ))
        .billAccountingCount(writeBillAccounting(billProcessingDatasets.billAccounting, batch))
        .billTaxCount(writeBillTaxes(billProcessingDatasets.billTaxes, batch))
        .pendingMinimumChargeCount(writePendingMinCharges(billProcessingDatasets.pendingMinimumCharges, batch))
        .minChargeBillsCount(writeMinimumChargeBills(billProcessingDatasets.minimumChargeBills, batch))
        .billableItemResultCount(new ResultCount(
            billableItemErrorIgnoredCount,
            billableItemErrorCount,
            billableItemCount,
            billableItemErrorFailedTodayCount,
            previouslyFailedBillableItemsCount
        ))
        .billRelationshipCount(correctedBillsCount)
        .build();
  }

  private Dataset<BillableItemRow> readBillableItems(Batch batch) {
    val standardBillableItems = standardBillableItemSource.load(spark, batch.id);
    val miscBillableItems = miscBillableItemSource.load(spark, batch.id);

    return standardBillableItems.union(miscBillableItems);
  }

  private long writePendingBills(Dataset<PendingBillRow> pendingBills, Batch batch) {
    return namedAction("Writing Pending Bills", sparkContext ->
        pendingBillWriter.write(batch.id, batch.createdAt, pendingBills));
  }

  private long writeCompleteBills(Dataset<BillRow> completeBills, Batch batch) {
    return namedAction("Writing Complete Bills", sparkContext ->
        completeBillWriter.write(batch.id, batch.createdAt, completeBills));
  }

  private long writeBillLineDetails(Dataset<BillLineDetailRow> billLineDetails, Batch batch) {
    return namedAction("Writing Bill Line Details", sparkContext ->
        billLineDetailWriter.write(batch.id, batch.createdAt, billLineDetails));
  }

  private long writeBillPrices(Dataset<Tuple2<String, BillPriceRow>> billPrices, Batch batch) {
    return namedAction("Writing Bill Price", sparkContext ->
        billPriceWriter.write(batch.id, batch.createdAt, billPrices));
  }

  private long writeFailedBillableItems(Dataset<FailedBillableItem> failedBillableItems, Batch batch) {
    return namedAction("Writing Failed Billable Items", sparkContext ->
        failedBillableItemWriter.write(batch.id, batch.createdAt, failedBillableItems));
  }

  private long writeCompletePreviouslyFailedBillableItems(Dataset<FailedBillableItem> completedPrevFailedBillableItems, Batch batch) {
    return namedAction("Writing Completed Previously Failed Billable Items", sparkContext ->
        completedPrevFailedBillableWriter.write(batch.id, batch.createdAt, completedPrevFailedBillableItems));
  }

  private long writeBillErrors(Dataset<BillError> billErrors, Batch batch) {
    return namedAction("Writing Error Bills", sparkContext ->
        billErrorWriter.write(batch.id, batch.createdAt, billErrors));
  }

  private long writeFixedBillErrors(Dataset<BillError> fixedBillErrors, Batch batch) {
    return namedAction("Writing Fixed Error Bills", sparkContext ->
        fixedBillErrorWriter.write(batch.id, batch.createdAt, fixedBillErrors));
  }

  private long writeBillTaxes(Dataset<BillTax> billTaxes, Batch batch) {
    return namedAction("Writing Bill Taxes", sparkContext -> billTaxWriter.write(batch.id, batch.createdAt, billTaxes));
  }

  private long writePendingMinCharges(Dataset<PendingMinChargeRow> pendingMinimumCharges, Batch batch) {
    return namedAction("Writing Pending Minimum Charges", sparkContext ->
        pendingMinChargeWriter.write(batch.id, batch.createdAt, pendingMinimumCharges));
  }

  private long writeMinimumChargeBills(Dataset<MinimumChargeBillRow> minimumChargeBills, Batch batch) {
    return namedAction("Writing Minimum Charge Bills", sparkContext ->
        minimumChargeBillWriter.write(batch.id, batch.createdAt, minimumChargeBills));
  }

  private long writeBillAccounting(Dataset<BillAccountingRow> billAccounting, Batch batch) {
    return namedAction("Writing Bill Accounting", sparkContext ->
        billAccountingWriter.write(batch.id, batch.createdAt, billAccounting));
  }

  private long writeCorrectedBillRelationship(Dataset<BillRelationshipRow> correctedBills, Batch batch) {
    return namedAction("Writing Corrected Bills Relationship", sparkContext ->
        billRelationshipWriter.write(batch.id, batch.createdAt, correctedBills));
  }

  @Data
  @Builder
  private static class BillProcessingDatasets {
    Dataset<FailedBillableItem> failedBillableItems;
    Dataset<FailedBillableItem> fixedBillableItems;
    Dataset<CompleteBillResult> completeBillResults;
    Dataset<PendingBillRow> successAndErrorPendingBills;
    Dataset<BillRow> successCompleteBills;
    Dataset<BillTax> billTaxes;
    Dataset<BillLineDetailRow> billLineDetails;
    Dataset<BillPriceRow> billPrices;
    Dataset<BillError> billErrors;
    Dataset<BillError> fixedBillError;
    Dataset<PendingMinChargeRow> pendingMinimumCharges;
    Dataset<MinimumChargeBillRow> minimumChargeBills;
    Dataset<BillAccountingRow> billAccounting;
    Dataset<BillRelationshipRow> relationshipBillResults;
  }
}