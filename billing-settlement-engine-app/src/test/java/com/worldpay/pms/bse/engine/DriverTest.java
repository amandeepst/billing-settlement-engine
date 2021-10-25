package com.worldpay.pms.bse.engine;

import static com.worldpay.pms.bse.engine.samples.TaxRelatedSamples.PARTIES;
import static com.worldpay.pms.bse.engine.samples.TaxRelatedSamples.PRODUCT_CHARACTERISTICS;
import static com.worldpay.pms.bse.engine.samples.TaxRelatedSamples.TAX_RATES;
import static com.worldpay.pms.bse.engine.samples.TaxRelatedSamples.TAX_RULES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.worldpay.pms.bse.domain.account.AccountDeterminationService;
import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountDetails;
import com.worldpay.pms.bse.domain.account.AccountRepository;
import com.worldpay.pms.bse.domain.account.BillingAccount;
import com.worldpay.pms.bse.domain.account.BillingAccount.BillingAccountBuilder;
import com.worldpay.pms.bse.domain.account.BillingCycle;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.engine.data.BillingRepository;
import com.worldpay.pms.bse.engine.domain.billcorrection.BillCorrectionService;
import com.worldpay.pms.bse.engine.transformations.AccountDeterminationServiceFactory;
import com.worldpay.pms.bse.engine.transformations.BillProcessingServiceFactory;
import com.worldpay.pms.bse.engine.transformations.BillableItemProcessingServiceFactory;
import com.worldpay.pms.bse.engine.transformations.model.IdGenerator;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemLineRow;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemRow;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemServiceQuantityRow;
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
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import com.worldpay.pms.bse.engine.transformations.registry.ReaderRegistry;
import com.worldpay.pms.bse.engine.transformations.registry.WriterRegistry;
import com.worldpay.pms.bse.engine.transformations.writers.InMemoryErrorWriter;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.batch.Batch.Watermark;
import com.worldpay.pms.spark.core.factory.ExecutorLocalFactory;
import com.worldpay.pms.spark.core.factory.Factory;
import com.worldpay.pms.testing.junit.SparkContext;
import com.worldpay.pms.testing.stereotypes.WithSpark;
import com.worldpay.pms.testing.utils.InMemoryDataSource;
import com.worldpay.pms.testing.utils.InMemoryWriter;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.val;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

@WithSpark
class DriverTest {

  private InMemoryDataSource.Builder<BillableItemRow> billableItemSourceBuilder;
  private InMemoryDataSource.Builder<BillableItemRow> miscBillableItemSourceBuilder;
  private InMemoryDataSource.Builder<PendingBillRow> pendingBillSourceBuilder;
  private InMemoryDataSource.Builder<PendingMinChargeRow> pendingMinChargeSourceBuilder;

  private InMemoryDataSource.Builder<Tuple2<InputBillCorrectionRow, BillLineRow[]>> billCorrectionSourceBuilder;
  private InMemoryDataSource.Builder<BillTax> billTaxSourceBuilder;

  private InMemoryWriter<PendingBillRow> pendingBillWriter;
  private InMemoryWriter<BillRow> completeBillWriter;
  private InMemoryWriter<BillLineDetailRow> billLineDetailWriter;
  private InMemoryErrorWriter<FailedBillableItem> failedBillableItemWriter;
  private InMemoryWriter<FailedBillableItem> fixedBillableItemWriter;
  private InMemoryWriter<Tuple2<String, BillPriceRow>> billPriceWriter;

  private InMemoryErrorWriter<BillError> billErrorWriter;
  private InMemoryWriter<BillError> fixedBillErrorWriter;
  private InMemoryWriter<BillTax> billTaxWriter;
  private InMemoryWriter<PendingMinChargeRow> pendingMinChargeWriter;
  private InMemoryWriter<BillAccountingRow> billAccountingWriter;
  private InMemoryWriter<BillRelationshipRow> billRelationshipWriter;
  private InMemoryWriter<MinimumChargeBillRow> minimumChargeBillWriter;

  private InMemoryAccountRepository.InMemoryAccountRepositoryBuilder inMemoryAccountRepository;
  private InMemoryBillingRepository.InMemoryBillingRepositoryBuilder inMemoryBillingRepository;
  private InMemoryBillIdGenerator.InMemoryBillIdGeneratorBuilder inMemoryBillIdGenerator;

  public final static Supplier<BillingAccountBuilder> BILLING_ACCOUNT = () -> BillingAccount.builder()
      .billingCycle(new BillingCycle("WPMO", LocalDate.of(2020, 11, 10), LocalDate.of(2020, 12, 10)))
      .accountId("123")
      .currency("EUR")
      .accountType("CHRG")
      .businessUnit("BU1")
      .legalCounterparty("P00010010")
      .childPartyId("P00010001")
      .partyId("P00010000")
      .subAccountType("CHRG");

  private final static List<BillingCycle> BILLING_CYCLES = Collections.singletonList(
      new BillingCycle("WPMO", LocalDate.of(2021, 1, 1), LocalDate.of(2021, 1, 31)));

  private final static List<AccountDetails> ACCOUNT_DETAILS =
      Collections.singletonList(new AccountDetails("123", "", "WPMO"));

  @AfterAll
  static void afterAll() {
    SparkContext.reset();
  }

  @BeforeEach
  void setUp(SparkSession spark) {
    this.billableItemSourceBuilder = InMemoryDataSource.builder(Encodings.BILLABLE_ITEM_ROW_ENCODER);
    this.miscBillableItemSourceBuilder = InMemoryDataSource.builder(Encodings.BILLABLE_ITEM_ROW_ENCODER);
    this.pendingBillSourceBuilder = InMemoryDataSource.builder(Encodings.PENDING_BILL_ENCODER);
    this.pendingMinChargeSourceBuilder = InMemoryDataSource.builder(InputEncodings.PENDING_MIN_CHARGE_ROW_ENCODER);
    this.billCorrectionSourceBuilder = InMemoryDataSource.builder(InputEncodings.BILL_CORRECTION_ROW_ENCODER);
    this.billTaxSourceBuilder = InMemoryDataSource.builder(Encodings.BILL_TAX);

    this.pendingBillWriter = new InMemoryWriter<>();
    this.completeBillWriter = new InMemoryWriter<>();
    this.billLineDetailWriter = new InMemoryWriter<>();

    this.billErrorWriter = new InMemoryErrorWriter<>();
    this.fixedBillErrorWriter = new InMemoryWriter<>();
    this.failedBillableItemWriter = new InMemoryErrorWriter<>();
    this.fixedBillableItemWriter = new InMemoryWriter<>();
    this.billPriceWriter = new InMemoryWriter<>();

    this.billTaxWriter = new InMemoryWriter<>();
    this.pendingMinChargeWriter = new InMemoryWriter<>();
    this.billAccountingWriter = new InMemoryWriter<>();
    this.billRelationshipWriter = new InMemoryWriter<>();
    this.minimumChargeBillWriter = new InMemoryWriter<>();

    this.inMemoryAccountRepository = InMemoryAccountRepository.builder();
    inMemoryAccountRepository.addAccount(BILLING_ACCOUNT.get().subAccountId("test").build());
    inMemoryAccountRepository.addAccount(BILLING_ACCOUNT.get().subAccountId("2").build());
    BILLING_CYCLES.forEach(billingCycle -> inMemoryAccountRepository.addBillingCycle(billingCycle));
    ACCOUNT_DETAILS.forEach(accountDetails -> inMemoryAccountRepository.addAccountDetail(accountDetails));

    this.inMemoryBillingRepository = InMemoryBillingRepository.builder();
    TAX_RATES.forEach(taxRate -> inMemoryBillingRepository.addTaxRate(taxRate));
    TAX_RULES.forEach(taxRule -> inMemoryBillingRepository.addTaxRule(taxRule));
    PARTIES.forEach(party -> inMemoryBillingRepository.addParty(party));
    PRODUCT_CHARACTERISTICS.forEach(productCharacteristic -> inMemoryBillingRepository.addProductCharacteristic(productCharacteristic));
    this.inMemoryBillIdGenerator = InMemoryBillIdGenerator.builder();
  }

  @Test
  void whenNoTransactionsThenJobCompletesSuccessfullyAndNoDataIsPublished(SparkSession spark) {
    // when we run on an empty data source
    BillingBatchRunResult result = createDriverAndRun(spark,
        billableItemSourceBuilder.build(),
        miscBillableItemSourceBuilder.build(),
        pendingBillSourceBuilder.build(),
        pendingMinChargeSourceBuilder.build(),
        billCorrectionSourceBuilder.build(),
        billTaxSourceBuilder.build(),
        pendingBillWriter,
        completeBillWriter,
        billLineDetailWriter,
        billPriceWriter,
        billErrorWriter,
        fixedBillErrorWriter,
        failedBillableItemWriter,
        fixedBillableItemWriter,
        billTaxWriter,
        pendingMinChargeWriter,
        billAccountingWriter,
        billRelationshipWriter,
        minimumChargeBillWriter,
        LocalDate.parse("2020-02-02"),
        Collections.emptyList(),
        Collections.singletonList("STANDARD"));

    // then no outputs are generated
    assertThat(pendingBillWriter.count(), is(0L));
    assertThat(result.getSuccessTransactionsCount(), is(0L));
    assertThat(result.getErrorTransactionsCount(), is(0L));
    assertThat(result.getBillableItemResultCount().getTotalCount(), is(0L));

    assertEquals(1L, result.getRunId());
  }

  @Test
  void whenBillableItemDatasetIsPopulatedThenPendingBillWillBePopulated(SparkSession spark) {
    getTestBillableItems().forEach(billableItem -> billableItemSourceBuilder.addRows(billableItem));
    InMemoryDataSource<BillableItemRow> localBillableItemSource = billableItemSourceBuilder.build();

    BillingBatchRunResult result = createDriverAndRun(spark,
        localBillableItemSource,
        miscBillableItemSourceBuilder.build(),
        pendingBillSourceBuilder.build(),
        pendingMinChargeSourceBuilder.build(),
        billCorrectionSourceBuilder.build(),
        billTaxSourceBuilder.build(),
        pendingBillWriter,
        completeBillWriter,
        billLineDetailWriter,
        billPriceWriter,
        billErrorWriter,
        fixedBillErrorWriter,
        failedBillableItemWriter,
        fixedBillableItemWriter,
        billTaxWriter,
        pendingMinChargeWriter,
        billAccountingWriter,
        billRelationshipWriter,
        minimumChargeBillWriter,
        LocalDate.parse("2019-02-02"),
        Collections.emptyList(),
        Collections.singletonList("STANDARD"));

    assertEquals(2, result.getPendingBillCount());
    List<PendingBillRow> pendingBills = pendingBillWriter.collectAsList();
    assertEquals(2, pendingBills.size());
    PendingBillRow firstPendingBill = pendingBills.get(0);
    PendingBillRow secondPendingBill = pendingBills.get(1);

    assertEquals("N", firstPendingBill.getAdhocBillIndicator());
    PendingBillLineRow[] pendingBillLines = firstPendingBill.getPendingBillLines();
    assertEquals(1, pendingBillLines.length);
    assertEquals("EUR", pendingBillLines[0].getFundingCurrency());
    assertTrue(Arrays.stream(pendingBillLines[0].getPendingBillLineCalculations()).allMatch(x -> x.getIncludeOnBill().equals("N")));
    // one billable item is a refund
    assertThat(
        firstPendingBill.getPendingBillLines()[0].getFundingAmount()
            .add(secondPendingBill.getPendingBillLines()[0].getFundingAmount())
            .compareTo(BigDecimal.ZERO),
        is(0));
    assertThat(result.getPendingBillCount(), is(2L));
  }

  @Test
  void whenBillableItemDatasetIsPopulatedThenPendingBillWillAggregate(SparkSession spark) {
    getBillableItemsThatAggregate().forEach(billableItem -> billableItemSourceBuilder.addRows(billableItem));
    InMemoryDataSource<BillableItemRow> localBillableItemSource = billableItemSourceBuilder.build();

    BillingBatchRunResult result = createDriverAndRun(spark,
        localBillableItemSource,
        miscBillableItemSourceBuilder.build(),
        pendingBillSourceBuilder.build(),
        pendingMinChargeSourceBuilder.build(),
        billCorrectionSourceBuilder.build(),
        billTaxSourceBuilder.build(),
        pendingBillWriter,
        completeBillWriter,
        billLineDetailWriter,
        billPriceWriter,
        billErrorWriter,
        fixedBillErrorWriter,
        failedBillableItemWriter,
        fixedBillableItemWriter,
        billTaxWriter,
        pendingMinChargeWriter,
        billAccountingWriter,
        billRelationshipWriter,
        minimumChargeBillWriter,
        LocalDate.parse("2020-02-02"),
        Collections.emptyList(),
        Collections.singletonList("STANDARD"));

    assertEquals(1, result.getPendingBillCount());

    PendingBillLineRow aggPendingBillLine = pendingBillWriter.collectAsList().get(0).getPendingBillLines()[0];
    assertThat(aggPendingBillLine.getFundingAmount().abs(), is(new BigDecimal("20.000000000000000000")));
    assertThat(aggPendingBillLine.getQuantity().abs(), is(new BigDecimal("2.000000000000000000")));
    assertThat(aggPendingBillLine.getTransactionAmount(), is(new BigDecimal("20.000000000000000000")));
  }

  @Test
  void whenBillableItemDatasetIsPopulatedAndValidationFailsThenPendingBillDatasetWillBeEmpty(SparkSession spark) {
    getTestBillableItems().forEach(billableItem -> billableItemSourceBuilder.addRows(billableItem));
    InMemoryDataSource<BillableItemRow> localBillableItemSource = billableItemSourceBuilder.build();

    this.inMemoryAccountRepository = InMemoryAccountRepository.builder();
    inMemoryAccountRepository.addAccount(BILLING_ACCOUNT.get().subAccountId("1").build());
    inMemoryAccountRepository.addAccount(BILLING_ACCOUNT.get().subAccountId("2").build());

    BillingBatchRunResult result = createDriverAndRun(spark,
        localBillableItemSource,
        miscBillableItemSourceBuilder.build(),
        pendingBillSourceBuilder.build(),
        pendingMinChargeSourceBuilder.build(),
        billCorrectionSourceBuilder.build(),
        billTaxSourceBuilder.build(),
        pendingBillWriter,
        completeBillWriter,
        billLineDetailWriter,
        billPriceWriter,
        billErrorWriter,
        fixedBillErrorWriter,
        failedBillableItemWriter,
        fixedBillableItemWriter,
        billTaxWriter,
        pendingMinChargeWriter,
        billAccountingWriter,
        billRelationshipWriter,
        minimumChargeBillWriter,
        LocalDate.parse("2019-02-02"),
        Collections.emptyList(),
        Collections.singletonList("STANDARD"));

    assertThat(result.getBillableItemResultCount().getErrorCount(), is(2L));
  }

  //
  // helper functions
  //
  private List<BillableItemRow> getTestBillableItems() {
    Map<String, BigDecimal> serviceQuantities = new HashMap<>();
    serviceQuantities.put("TXN_VOL", BigDecimal.ONE);
    serviceQuantities.put("F_M_AMT", BigDecimal.ONE);
    BillableItemServiceQuantityRow billableItemServiceQuantityRow = new BillableItemServiceQuantityRow("rateSchedule", serviceQuantities);

    BillableItemLineRow billableItemLineRow = new BillableItemLineRow(
        "class_1",
        BigDecimal.ONE,
        "F_M_AMT",
        "N/A",
        BigDecimal.ONE,
        1
    );

    val billableItemRow1 = new BillableItemRow("billItemId1",
        "test", "lcp", Date.valueOf("2021-01-01"), "0101010101", "sett_lvl_type|sett_lvl_val", "gran_kv", "EUR", "EUR", "EUR", "EUR",
        "EUR", "1849326380", "PREMIUM", "PRVCCL01", 0, "merchantCode1", "123", -1, "N", "STANDARD_BILLABLE_ITEM", "N", "N", "N", "N", "N", "N", "N",
        null, null,
        null, null, null, 0, Date.valueOf("2021-01-01"),
        new BillableItemLineRow[]{billableItemLineRow}, billableItemServiceQuantityRow);

    val billableItemRow2 = new BillableItemRow("billItemId2",
        "test", "lcp", Date.valueOf("2021-01-01"), "0101010102", "invalid_sett_val", "gran_kv", "EUR", "EUR", "EUR", "EUR", "EUR",
        "1849326380", "PREMIUM", "PRVCCL01", -1, "merchantCode2", "123", -1, "N", "STANDARD_BILLABLE_ITEM", "N", "N", "N", "N", "N", "N", "N",
        null, null,
        null, null, null, 0, Date.valueOf("2021-01-01"),
        new BillableItemLineRow[]{billableItemLineRow}, billableItemServiceQuantityRow);

    return Arrays.asList(billableItemRow1, billableItemRow2);
  }

  private List<BillableItemRow> getBillableItemsThatAggregate() {
    Map<String, BigDecimal> serviceQuantities = new HashMap<>();
    serviceQuantities.put("TXN_VOL", BigDecimal.ONE);
    serviceQuantities.put("TXN_AMT", BigDecimal.TEN);
    serviceQuantities.put("F_M_AMT", BigDecimal.TEN);

    BillableItemServiceQuantityRow billableItemServiceQuantityRow = new BillableItemServiceQuantityRow("rateSchedule", serviceQuantities);

    BillableItemLineRow billableItemLineRow = new BillableItemLineRow(
        "class_1",
        BigDecimal.ONE,
        "F_M_AMT",
        "N/A",
        BigDecimal.ONE,
        1
    );

    val billableItemRow1 = new BillableItemRow("billItemId1",
        "test", "lcp", Date.valueOf("2021-01-01"), "0101010101", "sett_lvl_type", "gran_kv", "EUR", "EUR", "EUR", "EUR",
        "EUR", "1849326380", "PREMIUM", "PRVCCL01", 0, null, "121", -1, "N", "STANDARD_BILLABLE_ITEM", "N", "N", "N", "N", "N", "N", "N",
        null, null,
        null, null, null, 0, Date.valueOf("2021-01-01"),
        new BillableItemLineRow[]{billableItemLineRow}, billableItemServiceQuantityRow);

    val billableItemRow2 = new BillableItemRow("billItemId2",
        "test", "lcp", Date.valueOf("2021-01-01"), "0101010101", "sett_lvl_type", "gran_kv", "EUR", "EUR", "EUR", "EUR", "EUR",
        "1849326380", "PREMIUM", "PRVCCL01", 0, null, "123", -1, "N", "STANDARD_BILLABLE_ITEM", "N", "N", "N", "N", "N", "N", "N", null,
        null, null,
        null, null, 0, Date.valueOf("2021-01-01"), new BillableItemLineRow[]{billableItemLineRow}, billableItemServiceQuantityRow);

    return Arrays.asList(billableItemRow1, billableItemRow2);
  }

  BillingBatchRunResult createDriverAndRun(SparkSession sparkSession,
      InMemoryDataSource<BillableItemRow> billableItemSource, InMemoryDataSource<BillableItemRow> miscBillableItemSource,
      InMemoryDataSource<PendingBillRow> pendingBillSource, InMemoryDataSource<PendingMinChargeRow> pendingMinChargeSource,
      InMemoryDataSource<Tuple2<InputBillCorrectionRow, BillLineRow[]>> billCorrectionSource, InMemoryDataSource<BillTax> billTaxSource,
      InMemoryWriter<PendingBillRow> pendingBillWriter,
      InMemoryWriter<BillRow> completeBillWriter, InMemoryWriter<BillLineDetailRow> billLineDetailWriter,
      InMemoryWriter<Tuple2<String, BillPriceRow>> billPriceWriter, InMemoryErrorWriter<BillError> billErrorWriter,
      InMemoryWriter<BillError> fixedBillErrorWriter, InMemoryErrorWriter<FailedBillableItem> failedBillableItemWriter,
      InMemoryWriter<FailedBillableItem> fixedBillableItemWriter, InMemoryWriter<BillTax> billTaxWriter,
      InMemoryWriter<PendingMinChargeRow> pendingMinChargeWriter,
      InMemoryWriter<BillAccountingRow> billAccountingWriter,
      InMemoryWriter<BillRelationshipRow> billRelationshipWriter,
      InMemoryWriter<MinimumChargeBillRow> minimumChargeBillWriter,
      LocalDate logicalDate, List<String> processingGroups,
      List<String> billingTypes) {

    BillingConfiguration billingConfiguration = new BillingConfiguration();
    Factory<AccountRepository> accountRepositoryFactory = new DummyFactory<>(inMemoryAccountRepository.build());
    Factory<AccountDeterminationService> accountDeterminationServiceFactory = ExecutorLocalFactory.of(
        new AccountDeterminationServiceFactory(accountRepositoryFactory, billingConfiguration.getBilling()));
    Factory<BillingRepository> billingRepositoryFactory = new DummyFactory<>(
        inMemoryBillingRepository.logicalDate(Date.valueOf(logicalDate)).build());
    Factory<BillCorrectionService> billCorrectionServiceFactory = new DummyFactory<>(new BillCorrectionService());
    Factory<IdGenerator> idGeneratorFactory = new DummyFactory<>(inMemoryBillIdGenerator.build());

    Driver billingDriver = new Driver(
        sparkSession,
        id -> 1L,
        new ReaderRegistry(
            billableItemSource,
            miscBillableItemSource,
            pendingBillSource,
            pendingMinChargeSource,
            billCorrectionSource,
            billTaxSource),
        new WriterRegistry(
            pendingBillWriter,
            completeBillWriter,
            billLineDetailWriter,
            billErrorWriter,
            fixedBillErrorWriter,
            failedBillableItemWriter,
            fixedBillableItemWriter,
            billPriceWriter,
            billTaxWriter,
            pendingMinChargeWriter,
            billAccountingWriter,
            billRelationshipWriter,
            minimumChargeBillWriter
        ),
        new BillableItemProcessingServiceFactory(accountDeterminationServiceFactory),
        new BillProcessingServiceFactory(accountDeterminationServiceFactory, billingRepositoryFactory, logicalDate, processingGroups,
            billingTypes, billingConfiguration.getBilling()),
        billingRepositoryFactory,
        billingConfiguration.getDriverConfig(),
        billCorrectionServiceFactory, idGeneratorFactory);

    return billingDriver.run(getGenericBatch());
  }

  Batch getGenericBatch() {
    Watermark watermark = new Watermark(LocalDateTime.MIN, LocalDateTime.MAX);
    BatchId id = new BatchId(watermark, 1);
    LocalDateTime createdAt = LocalDateTime.now();

    return new Batch(id, null, createdAt, "", null);
  }
}
