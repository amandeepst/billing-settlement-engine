package com.worldpay.pms.pba.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.worldpay.pms.pba.engine.data.PostBillingRepository;
import com.worldpay.pms.pba.domain.WafProcessingService;
import com.worldpay.pms.pba.domain.model.BillAdjustment;
import com.worldpay.pms.pba.domain.model.WithholdFunds;
import com.worldpay.pms.pba.domain.model.WithholdFundsBalance;
import com.worldpay.pms.pba.engine.model.input.BillRow;
import com.worldpay.pms.pba.engine.model.input.BillRow.BillRowBuilder;
import com.worldpay.pms.pba.engine.model.output.BillAdjustmentAccountingRow;
import com.worldpay.pms.pba.engine.transformations.WafProcessingServiceFactory;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.batch.Batch.Watermark;
import com.worldpay.pms.spark.core.factory.ExecutorLocalFactory;
import com.worldpay.pms.spark.core.factory.Factory;
import com.worldpay.pms.testing.stereotypes.WithSpark;
import com.worldpay.pms.testing.utils.InMemoryDataSource;
import com.worldpay.pms.testing.utils.InMemoryWriter;
import io.vavr.collection.List;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

@WithSpark
class DriverTest {

  private InMemoryDataSource.Builder<BillRow> billSourceBuilder;
  private InMemoryWriter<Tuple2<String, BillAdjustment>> postBillAdjWriter;
  private InMemoryWriter<BillAdjustmentAccountingRow> postBillAdjAccountingWriter;

  private final AtomicLong counter = new AtomicLong(1);

  private InMemoryPostBillingRepository.InMemoryPostBillingRepositoryBuilder inMemoryPostBillingRepositoryBuilder;

  @BeforeEach
  void setUp(SparkSession spark) {
    this.billSourceBuilder = InMemoryDataSource.builder(Encodings.BILL_ROW_ENCODER);
    this.postBillAdjWriter = new InMemoryWriter<>();
    this.postBillAdjAccountingWriter = new InMemoryWriter<>();
    this.inMemoryPostBillingRepositoryBuilder =  InMemoryPostBillingRepository.builder();
  }

  @Test
  @DisplayName("When no bill then no adjustment is published")
  void whenNoBillsThenJobCompletesSuccessfullyAndNoAdjustmentIsPublished(SparkSession spark) {
    // when we run on an empty data source
    PostBillingBatchRunResult result = createDriverAndRun(spark,
        billSourceBuilder.build(),
        postBillAdjWriter,
        postBillAdjAccountingWriter,
        inMemoryPostBillingRepositoryBuilder.build());

    assertEquals(0L, result.getErrorTransactionsCount());
    assertEquals(0L, result.getBillCount());
  }

  @Test
  @DisplayName("When some bills then publish the right adjustments")
  void whenBillsThenJobCompletesSuccessfullyAndAdjustmentsArePublished(SparkSession spark) {
    getInputBillRows().forEach(row -> billSourceBuilder.addRows(row));
    inMemoryPostBillingRepositoryBuilder.addWithholdFund(
        new WithholdFunds("0215874", "WAF", null, 100d));
    inMemoryPostBillingRepositoryBuilder.addWafSubAccountId(new io.vavr.Tuple2<>("0215874", "214587"));
    inMemoryPostBillingRepositoryBuilder.addWafBalance(
        new WithholdFundsBalance("P00007", "P00001", "GPB", BigDecimal.valueOf(-140)));

    PostBillingBatchRunResult result = createDriverAndRun(spark,
        billSourceBuilder.build(),
        postBillAdjWriter,
        postBillAdjAccountingWriter,
        inMemoryPostBillingRepositoryBuilder.build());

    assertEquals(0L, result.getErrorTransactionsCount());
    assertEquals(2L, result.getBillCount());
    assertEquals(2L, result.getAdjustmentCount());
    java.util.List<Tuple2<String, BillAdjustment>> billAdjustments = postBillAdjWriter.collectAsList();
    assertEquals(2L,  billAdjustments.size());
    assertEquals(4L, result.getAdjustmentAccountingCount());
    java.util.List<BillAdjustmentAccountingRow> billAdjustmentsAccounting = postBillAdjAccountingWriter.collectAsList();
    assertEquals(4L, billAdjustmentsAccounting.size());
  }

  private List<BillRow> getInputBillRows(){
    Supplier<BillRowBuilder> bill = () -> BillRow.builder()
        .billId("bill1")
        .billNumber("123")
        .billSubAccountId("1234")
        .accountType("FUND")
        .accountId("0215874")
        .businessUnit("WPBU")
        .billDate(Date.valueOf("2021-02-17"))
        .billCycleId("WPDY")
        .startDate(Date.valueOf("2021-02-17"))
        .endDate(Date.valueOf("2021-02-17"))
        .billReference("REF")
        .status("STATUS")
        .adhocBillFlag("N")
        .releaseReserveIndicator("N")
        .releaseWafIndicator("N")
        .fastestPaymentRouteIndicator("N")
        .individualBillIndicator("N")
        .manualNarrative(" ")
        .partyId("P00007")
        .legalCounterparty("P00001")
        .billAmount(BigDecimal.valueOf(-100L))
        .subAccountType("FUND")
        .currencyCode("GPB");

    return List.of(bill.get().build(), bill.get().billAmount(BigDecimal.valueOf(-120L)).build());
  }

  PostBillingBatchRunResult createDriverAndRun(SparkSession sparkSession, InMemoryDataSource<BillRow> billSource,
      InMemoryWriter<Tuple2<String, BillAdjustment>> postBillAdjWriter,
      InMemoryWriter<BillAdjustmentAccountingRow> postBillAdjAccountingWriter,
      InMemoryPostBillingRepository postBillingRepository) {

    PostBillingConfiguration billingConfiguration = new PostBillingConfiguration();

    Factory<PostBillingRepository> accountRepositoryFactory = new DummyFactory<PostBillingRepository>
        (postBillingRepository);
    Factory<WafProcessingService> processingServiceFactory = ExecutorLocalFactory.of(
        new WafProcessingServiceFactory(accountRepositoryFactory,billingConfiguration.getStore()));

    Driver billingDriver = new Driver(
        sparkSession,
        id -> counter.incrementAndGet(),
        billSource,
        postBillAdjWriter,
        postBillAdjAccountingWriter, processingServiceFactory);

    return billingDriver.run(getGenericBatch());
  }

  Batch getGenericBatch() {
    Watermark watermark = new Watermark(LocalDateTime.MIN, LocalDateTime.MAX);
    BatchId id = new BatchId(watermark, 1);
    LocalDateTime createdAt = LocalDateTime.now();

    return new Batch(id, null, createdAt, "", null);
  }
}