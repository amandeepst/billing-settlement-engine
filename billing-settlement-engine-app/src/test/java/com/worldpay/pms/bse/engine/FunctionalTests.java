package com.worldpay.pms.bse.engine;

import static com.worldpay.pms.bse.domain.common.Utils.getDate;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.ACCOUNT_DETAILS_1;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.ACCOUNT_DETAILS_2;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.ACCOUNT_DETAILS_3;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.ACCOUNT_DETAILS_4;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.ACCOUNT_DETAILS_5;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.ACCOUNT_DETAILS_6;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.ACCOUNT_DETAILS_7;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_11;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_12;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_21;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_31;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_32;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_33;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_34;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_40;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_41;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_42;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_43;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_52_FAILED;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_52_FIXED;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_53_FAILED;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_53_FIXED;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_CHRG_11;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_CHRG_12;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_CHRG_21;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLABLE_ITEM_CHRG_22;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLING_ACCOUNT_1;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLING_ACCOUNT_2;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLING_ACCOUNT_3;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLING_ACCOUNT_4;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLING_ACCOUNT_5;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLING_ACCOUNT_6;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLING_ACCOUNT_7;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILLING_ACCOUNT_8;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILL_LINE_ROW_1;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILL_LINE_ROW_2;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.BILL_TAX_1;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.DATE_2021_01_01;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.DATE_2021_01_26;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.DATE_2021_01_27;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.DATE_2021_01_28;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.DATE_2021_01_30;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.DATE_2021_01_31;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.DATE_2021_02_01;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.EMPTY_PROCESSING_GROUP;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.INPUT_BILL_CORRECTION_ROW_1;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.INPUT_BILL_CORRECTION_ROW_2;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.MINIMUM_CHARGE_1;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.PENDING_MINIMUM_CHARGE_1;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.STANDARD_AND_ADHOC_BILLING;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.STANDARD_BILLING;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.WPDY;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.WPDY_20210127;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.WPDY_20210128;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.WPDY_20210131;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.WPDY_20210201;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.WPMO;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.WPMO_202101;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.WPMO_202102;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.changeBillCycleCode;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.changeBillableItemAccruedDate;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.getPendingBillFrom;
import static com.worldpay.pms.bse.engine.FunctionalTestsData.removeBillLineDetails;
import static com.worldpay.pms.bse.engine.samples.TaxRelatedSamples.FRAVI_TX_L_ROU;
import static com.worldpay.pms.bse.engine.samples.TaxRelatedSamples.MINCHRGP_TX_L_ROU;
import static com.worldpay.pms.bse.engine.samples.TaxRelatedSamples.PARTY_ROU_INT_EU_N;
import static com.worldpay.pms.bse.engine.samples.TaxRelatedSamples.RATE_ROU_L;
import static com.worldpay.pms.bse.engine.samples.TaxRelatedSamples.R_ROU_SST;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.worldpay.pms.bse.domain.account.AccountDeterminationService;
import com.worldpay.pms.bse.domain.account.AccountRepository;
import com.worldpay.pms.bse.domain.common.ErrorCatalog;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.engine.data.BillingRepository;
import com.worldpay.pms.bse.engine.domain.billcorrection.BillCorrectionService;
import com.worldpay.pms.bse.engine.domain.billcorrection.BillCorrectionServiceFactory;
import com.worldpay.pms.bse.engine.transformations.AccountDeterminationServiceFactory;
import com.worldpay.pms.bse.engine.transformations.BillProcessingServiceFactory;
import com.worldpay.pms.bse.engine.transformations.BillableItemProcessingServiceFactory;
import com.worldpay.pms.bse.engine.transformations.model.IdGenerator;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemRow;
import com.worldpay.pms.bse.engine.transformations.model.billaccounting.BillAccountingRow;
import com.worldpay.pms.bse.engine.transformations.model.billerror.BillError;
import com.worldpay.pms.bse.engine.transformations.model.billerror.BillErrorDetail;
import com.worldpay.pms.bse.engine.transformations.model.billprice.BillPriceRow;
import com.worldpay.pms.bse.engine.transformations.model.billrelationship.BillRelationshipRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetailRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import com.worldpay.pms.bse.engine.transformations.model.failedbillableitem.FailedBillableItem;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillCorrectionRow;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.MinimumChargeBillRow;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.PendingMinChargeRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineCalculationRow;
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
import com.worldpay.pms.testing.stereotypes.WithSparkHeavyUsage;
import com.worldpay.pms.testing.utils.InMemoryDataSource;
import com.worldpay.pms.testing.utils.InMemoryWriter;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.val;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

@WithSparkHeavyUsage
class FunctionalTests {

  private SparkSession sparkSession;

  private InMemoryDataSource.Builder<BillableItemRow> standardBillableItemSourceBuilder;
  private InMemoryDataSource.Builder<BillableItemRow> miscBillableItemSourceBuilder;

  private InMemoryDataSource.Builder<PendingBillRow> pendingBillSourceBuilder;
  private InMemoryDataSource.Builder<PendingMinChargeRow> pendingMinChargeSourceBuilder;

  private InMemoryDataSource.Builder<Tuple2<InputBillCorrectionRow, BillLineRow[]>> billCorrectionSourceBuilder;
  private InMemoryDataSource.Builder<BillTax> billTaxSourceBuilder;

  private InMemoryWriter<PendingBillRow> pendingBillWriter;
  private InMemoryWriter<BillRow> completeBillWriter;
  private InMemoryWriter<BillLineDetailRow> billLineDetailWriter;
  private InMemoryWriter<Tuple2<String, BillPriceRow>> billPriceWriter;

  private InMemoryErrorWriter<BillError> billErrorWriter;
  private InMemoryWriter<BillError> fixedBillErrorWriter;
  private InMemoryErrorWriter<FailedBillableItem> failedBillableItemWriter;
  private InMemoryWriter<FailedBillableItem> fixedBillableItemWriter;
  private InMemoryWriter<BillTax> billTaxWriter;
  private InMemoryWriter<PendingMinChargeRow> pendingMinChargeWriter;
  private InMemoryWriter<BillAccountingRow> billAccountingWriter;
  private InMemoryWriter<BillRelationshipRow> billRelationshipWriter;
  private InMemoryWriter<MinimumChargeBillRow> minimumChargeBillWriter;

  private InMemoryAccountRepository.InMemoryAccountRepositoryBuilder inMemoryAccountRepository;
  private InMemoryBillingRepository.InMemoryBillingRepositoryBuilder inMemoryBillingRepository;
  private InMemoryBillIdGenerator.InMemoryBillIdGeneratorBuilder inMemoryBillIdGenerator;

  @BeforeEach
  void setUp(SparkSession spark) {
    this.sparkSession = spark;

    standardBillableItemSourceBuilder = InMemoryDataSource.builder(Encodings.BILLABLE_ITEM_ROW_ENCODER);

    miscBillableItemSourceBuilder = InMemoryDataSource.builder(Encodings.BILLABLE_ITEM_ROW_ENCODER);
    pendingBillSourceBuilder = InMemoryDataSource.builder(Encodings.PENDING_BILL_ENCODER);
    pendingMinChargeSourceBuilder = InMemoryDataSource.builder(InputEncodings.PENDING_MIN_CHARGE_ROW_ENCODER);
    billCorrectionSourceBuilder = InMemoryDataSource.builder(InputEncodings.BILL_CORRECTION_ROW_ENCODER);
    billTaxSourceBuilder = InMemoryDataSource.builder(Encodings.BILL_TAX);

    pendingBillWriter = new InMemoryWriter<>();
    completeBillWriter = new InMemoryWriter<>();
    billLineDetailWriter = new InMemoryWriter<>();
    billPriceWriter = new InMemoryWriter<>();

    billErrorWriter = new InMemoryErrorWriter<>();
    fixedBillErrorWriter = new InMemoryWriter<>();

    failedBillableItemWriter = new InMemoryErrorWriter<>();
    fixedBillableItemWriter = new InMemoryWriter<>();

    billTaxWriter = new InMemoryWriter<>();
    pendingMinChargeWriter = new InMemoryWriter<>();
    billAccountingWriter = new InMemoryWriter<>();
    billRelationshipWriter = new InMemoryWriter<>();
    minimumChargeBillWriter = new InMemoryWriter<>();

    inMemoryAccountRepository = InMemoryAccountRepository.builder();
    inMemoryAccountRepository.addAccount(BILLING_ACCOUNT_1);
    inMemoryAccountRepository.addAccount(BILLING_ACCOUNT_2);
    inMemoryAccountRepository.addAccount(BILLING_ACCOUNT_3);
    inMemoryAccountRepository.addAccount(BILLING_ACCOUNT_4);
    inMemoryAccountRepository.addAccount(BILLING_ACCOUNT_5);
    inMemoryAccountRepository.addAccount(BILLING_ACCOUNT_6);
    inMemoryAccountRepository.addAccount(BILLING_ACCOUNT_7);
    inMemoryAccountRepository.addAccount(BILLING_ACCOUNT_8);
    inMemoryAccountRepository.addAccountDetail(ACCOUNT_DETAILS_1);
    inMemoryAccountRepository.addAccountDetail(ACCOUNT_DETAILS_2);
    inMemoryAccountRepository.addAccountDetail(ACCOUNT_DETAILS_3);
    inMemoryAccountRepository.addAccountDetail(ACCOUNT_DETAILS_4);
    inMemoryAccountRepository.addAccountDetail(ACCOUNT_DETAILS_5);
    inMemoryAccountRepository.addAccountDetail(ACCOUNT_DETAILS_6);
    inMemoryAccountRepository.addAccountDetail(ACCOUNT_DETAILS_7);
    inMemoryAccountRepository.addBillingCycle(WPDY_20210127);
    inMemoryAccountRepository.addBillingCycle(WPDY_20210128);
    inMemoryAccountRepository.addBillingCycle(WPDY_20210131);
    inMemoryAccountRepository.addBillingCycle(WPDY_20210201);
    inMemoryAccountRepository.addBillingCycle(WPMO_202101);
    inMemoryAccountRepository.addBillingCycle(WPMO_202102);

    inMemoryBillingRepository = InMemoryBillingRepository.builder();

    this.inMemoryBillIdGenerator = InMemoryBillIdGenerator.builder();
  }

  /*
   * 4.
   * Given a transaction with a daily bill cycle and Accrued_dt equal to the billing run date,
   * When billing runs,
   * Then a complete bill should be generated
   */
  @Test
  void when1BillableItemWithDailyBillCycleAndLogicalDateIsEqualToAccruedDateThenCompleteAs1Bill() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_11);

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(0, 1, 1, 0, 0, 0, 0, 0, 1);
  }

  /*
   * 5.
   * Given a transaction with a daily bill cycle and Accrued_dt greater than the billing run date,
   * When billing runs,
   * Then no complete bill should be generated
   */
  @Test
  void when1BillableItemWithDailyBillCycleAndLogicalDateIsBeforeAccruedDateThenKeepAs1PendingBill() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_11);

    createDriverAndRun(DATE_2021_01_26, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(1, 0, 1, 0, 0, 0, 0, 0, 0);
  }

  /*
   * 6.
   * Given a transaction with a daily bill cycle and Accrued_dt less than the billing run date,
   * When billing runs,
   * Then a complete bill should be generated
   *
   * 20.
   * Given a transaction with Date Override N, the bill cycle is daily and billing runs for a date >= to the Accrued_dt,
   * When billing runs,
   * Then the bill should be generated
   */
  @Test
  void when1BillableItemWithDailyBillCycleAndLogicalDateIsAfterAccruedDateThenCompleteAs1Bill() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_11);

    createDriverAndRun(DATE_2021_01_28, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(0, 1, 1, 0, 0, 0, 0, 0, 1);
  }

  /*
   * 2 Billable Items with Daily Bill Cycle and different Aggregation Key are completed as 2 Bills
   */
  @Test
  void when2BillableItemsWithDifferentAggregationKeyAndDailyBillCycleThenCompleteAs2DifferentBills() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_11, BILLABLE_ITEM_21);

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(0, 2, 2, 0, 0, 0, 0, 0, 2);
  }

  /*
   * 3.
   * Given a transaction with a monthly bill cycle and Accrued_dt prior to the last day of the month,
   * When billing runs for a day prior to the last day of the month,
   * Then no complete bill should be generated
   */
  @Test
  void when1BillableItemWithMonthlyBillCycleAndLogicalDateIsNotEndOfBillCycleThenKeepAs1PendingBill() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_31);

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(1, 0, 1, 0, 0, 0, 0, 0, 0);
  }

  /*
   * 1.
   * Given a transaction with a monthly bill cycle, Accrued_dt = the last day of the month and an existing pending bill for the account,
   * When billing runs for the last day of the month,
   * Then the transaction needs to be summed up with the existing pending bill and generate a complete bill
   */
  @Test
  void when1BillableItemWithMonthlyBillCycleAndPendingBillExistsAndLogicalDateIsEndOfBillCycleThenAggregateAndCompleteAs1Bill() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_31);

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(1, 0, 1, 0, 0, 0, 0, 0, 0);

    pendingBillSourceBuilder.addRows(removeBillLineDetails(getRows(pendingBillWriter)));
    standardBillableItemSourceBuilder.clear().addRows(changeBillableItemAccruedDate(BILLABLE_ITEM_32, DATE_2021_01_31));

    createDriverAndRun(DATE_2021_01_31, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(0, 1, 1, 0, 0, 0, 0, 0, 4);

    BillRow completeBill = getCompleteBill();
    assertCompleteBillLineCount(completeBill, 2);
  }

  /*
   * 2.
   * Given a transaction with a monthly bill cycle and Accrued_dt prior to the last day of the month,
   * When billing runs for the last day of the month,
   * Then a complete bill needs to be generated
   */
  @Test
  void when1BillableItemWithMonthlyBillCycleAndLogicalDateIsEndOfBillCycleThenCompleteAs1Bill() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_31);

    createDriverAndRun(DATE_2021_01_31, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(0, 1, 1, 0, 0, 0, 0, 0, 2);
  }

  /*
   * 3BIS.
   * Given a transaction with a monthly bill cycle and Accrued_dt the last day of the month,
   * When billing runs for a day prior to the last day of the month,
   * Then no complete bill should be generated
   */
  @Test
  void when1BillableItemWithMonthlyBillCycleAndAccruedDateIsEndOfBillCycleAndLogicalDateIsBeforeEndOfBillCycleThenKeepAs1PendingBill() {
    standardBillableItemSourceBuilder.addRows(changeBillableItemAccruedDate(BILLABLE_ITEM_31, DATE_2021_01_31));

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(1, 0, 1, 0, 0, 0, 0, 0, 0);
  }

  /*
   * 8.
   * Given a transaction with a monthly bill cycle where Accrued_dt is prior to the billing run date and Adhoc flag is Y,
   * When running billing for a date prior to the last day of the month,
   * Then a complete bill should be generated
   *
   * Also, assert that misc bill prices are correctly accrued during aggregation
   * Also, assert that bill reference for misc bill items with individualPaymentIndicator=Y is correctly computed
   */
  @Test
  void when1BillableItemWithAdhocFlagAndMonthlyBillCycleAndLogicalDateIsBeforeEndOfBillCycleThenCompleteBill() {
    miscBillableItemSourceBuilder.addRows(BILLABLE_ITEM_41, BILLABLE_ITEM_42, BILLABLE_ITEM_40);

    createDriverAndRun(DATE_2021_01_28, EMPTY_PROCESSING_GROUP, STANDARD_AND_ADHOC_BILLING);

    assertOutputCounts(0, 2, 3, 0, 0, 3, 0, 0, 3);

    //bill granularity from misc billable item should be: billDate + accountId + miscGranularity
    BillRow bill41 = getCompleteBill(getBillLineDetail(BILLABLE_ITEM_41.getBillableItemId()).getBillId());
    BillPriceRow billPrice41 = getBillPrice(BILLABLE_ITEM_41.getBillableItemId());
    String expectedGranularity = "210131" + "9068590709" + BILLABLE_ITEM_41.getGranularity();
    String expectedReference = "9068590709" + "2021-01-01" + "YNNN";
    assertThat(bill41.getGranularity(), equalTo(expectedGranularity));
    assertThat(bill41.getBillReference(), equalTo(expectedReference));
    assertThat(billPrice41.getGranularity(), equalTo(expectedGranularity));

    BillRow bill40 = getCompleteBill(getBillLineDetail(BILLABLE_ITEM_40.getBillableItemId()).getBillId());
    BillPriceRow billPrice40 = getBillPrice(BILLABLE_ITEM_40.getBillableItemId());
    expectedGranularity = "210131" + "9068590709" + BILLABLE_ITEM_40.getGranularity();
    expectedReference = "9068590709" + "2021-01-01" + BILLABLE_ITEM_40.getBillableItemId() + "YNNN";
    assertThat(bill40.getGranularity(), equalTo(expectedGranularity));
    assertThat(bill40.getBillReference(), equalTo(expectedReference));
    assertThat(billPrice40.getGranularity(), equalTo(expectedGranularity));
  }

  /*
   * 9.
   * Given 2 transactions with the same key fields(except for Adhoc flag which is Y for the 1st and N for the 2nd)
   * and a monthly bill cycle where Accrued date <= billing run date,
   * When billing runs for a date prior to the last day of the month,
   * Then a bill should be generated for the 1st transaction while the 2nd transaction would only reach the pending bill
   */
  @Test
  void when2BillableItemsOneAdhocAndOneNotAndMonthlyBillCycleAndLogicalDateIsBeforeEndOfBillCycleThenCompleteOnlyAdhocBill() {
    miscBillableItemSourceBuilder.addRows(BILLABLE_ITEM_41, BILLABLE_ITEM_43);

    createDriverAndRun(DATE_2021_01_28, EMPTY_PROCESSING_GROUP, STANDARD_AND_ADHOC_BILLING);

    assertOutputCounts(1, 1, 2, 0, 0, 2, 0, 0, 1);

    assertThat(getPendingBill().getBillId(), equalTo(getBillLineDetail(BILLABLE_ITEM_43.getBillableItemId()).getBillId()));
    assertThat(getPendingBill().getBillCycleId(), equalTo(WPMO));
    assertThat(getPendingBill().getScheduleStart(), equalTo(getDate(DATE_2021_01_01)));
    assertThat(getPendingBill().getScheduleEnd(), equalTo(getDate(DATE_2021_01_31)));

    // Check that for adhoc bills we set bill date to logical date no matter the value of the bill cycle
    assertThat(getCompleteBill().getBillId(), equalTo(getBillLineDetail(BILLABLE_ITEM_41.getBillableItemId()).getBillId()));
    assertThat(getCompleteBill().getBillCycleId(), equalTo(WPDY));
    assertThat(getCompleteBill().getStartDate(), equalTo(getDate(DATE_2021_01_28)));
    assertThat(getCompleteBill().getEndDate(), equalTo(getDate(DATE_2021_01_28)));
    assertThat(getCompleteBill().getBillDate(), equalTo(getDate(DATE_2021_01_28)));
  }

  /*
   * 10.
   * Given 2 transactions with the same key fields(except for Adhoc flag which is Y for the 1st and N for the 2nd)
   * and a monthly bill cycle where Accrued date <= billing run date,
   * When billing runs for the last day of the month,
   * Then 2 bills should be generated for each transactions as the Adhoc flag is different
   */
  @Test
  void when2BillableItemsOneAdhocAndOneNotAndMonthlyBillCycleAndLogicalDateIsOnEndOfBillCycleThenCompleteAs2Bills() {
    miscBillableItemSourceBuilder.addRows(BILLABLE_ITEM_41, BILLABLE_ITEM_43);

    createDriverAndRun(DATE_2021_01_31, EMPTY_PROCESSING_GROUP, STANDARD_AND_ADHOC_BILLING);

    assertOutputCounts(0, 2, 2, 0, 0, 2, 0, 0, 2);
  }

  /*
   * 17.
   * Given a transaction with Date Override N, TXN accrued date > job parameter and a monthly bill cycle,
   * When billing runs for the last day of the month,
   * Then a complete bill should not be generated
   */
  @Test
  void when1BillableItemWithMonthlyBillCycleAndAccruedDateIsAfterLogicalDateAndLogicalDateIsEndOfBillCycleThenKeepAsPendingBill() {
    standardBillableItemSourceBuilder.addRows(changeBillableItemAccruedDate(BILLABLE_ITEM_31, DATE_2021_02_01));

    createDriverAndRun(DATE_2021_01_31, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(1, 0, 1, 0, 0, 0, 0, 0, 0);
  }

  /*
   * 18.
   * Given a transaction with Date Override N, TXN accrued date = the prior month that’s being run and a monthly bill cycle,
   * When billing runs,
   * Then a complete bill should be generated
   */
  @Test
  void when1BillableItemWithMonthlyBillCycleAndAccruedDateIsInPreviousBillCycleAndLogicalDateIsInCurrentBillCycleThenCompleteBill() {
    standardBillableItemSourceBuilder.addRows(changeBillableItemAccruedDate(BILLABLE_ITEM_31, DATE_2021_01_28));

    createDriverAndRun(DATE_2021_02_01, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(0, 1, 1, 0, 0, 0, 0, 0, 2);
  }

  /*
   * 19.
   * Given a transaction with Date Override N, the bill cycle is daily and billing runs for a date prior to the Accrued_dt,
   * When billing runs,
   * Then the bill should not be generated
   */
  @Test
  void when1BillableItemWithDailyBillCycleAndAccruedDateIsAfterLogicalDateThenKeepAsPendingBill() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_11);

    createDriverAndRun(DATE_2021_01_26, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(1, 0, 1, 0, 0, 0, 0, 0, 0);
  }

  /*
   * 2 Billable Items with Daily Bill Cycle and same Aggregation Key are completed as 1 Bill
   */
  @Test
  void when2BillableItemsWithSameAggregationKeyAndDailyBillCycleThenCompleteAs1Bill() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_11, BILLABLE_ITEM_12);

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(0, 1, 2, 0, 0, 0, 0, 0, 2);

    BillLineRow[] billLines = completeBillWriter.collectAsList().get(0).getBillLines();
    Stream.of(billLines).forEach(line -> assertThat(line.getMerchantCode(), is("merchantCode")));
  }

  /*
   * 2 Billable Items with Monthly Bill Cycle and same Aggregation Key and Logical Date is before End Date of Bill Cycle
   * are not completed, are kept as 1 Pending Bill
   */
  @Test
  void when2BillItemsWithSameAggregationKeyAndMonthlyBillCycleAndLogicalDateIsNotEndOfBillCycleThenKeepAs1PendingBill() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_31, BILLABLE_ITEM_32);

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(1, 0, 2, 0, 0, 0, 0, 0, 0);

    PendingBillRow pendingBill = getPendingBill();
    assertPendingBillLineCount(pendingBill, 2);
    assertPendingBillLineCalcCount(pendingBill, "9216814145", 2);
    assertPendingBillLineCalcCount(pendingBill, "9212202821", 2);
  }

  /*
   * 1 Billable Item and 1 PendingBill (previously failed) are completed as 1 Bill and an entry in BillItemError is added
   */
  @Test
  void whenCompletingAPendingBillThatWasInErrorThenFixedDateIsPresentInBillErrorEntry() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_11);
    pendingBillSourceBuilder.addRows(getPendingBillFrom(BILLABLE_ITEM_12, BILLING_ACCOUNT_1).toBuilder()
        .billId("1000000000001")
        .firstFailureOn(Date.valueOf("2021-01-01"))
        .retryCount((short) 2)
        .build());

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(0, 1, 2, 0, 0, 0, 1, 1, 0, 0, 2);
  }

  @Test
  void whenUpdating2PendingBillsAndAccountRepositoryDoesNotMatchTheProcessingGroupForTheAccountIdsThen2BillErrorsAreCreated() {
    pendingBillSourceBuilder
        .addRows(
            getPendingBillFrom(BILLABLE_ITEM_12, BILLING_ACCOUNT_1).toBuilder()
                .billId("1000000000001")
                .accountId("test_account")
                .build(),
            getPendingBillFrom(BILLABLE_ITEM_11, BILLING_ACCOUNT_1).toBuilder()
                .billId("1000000000002")
                .accountId("test_account_2")
                .firstFailureOn(Date.valueOf(DATE_2021_01_27))
                .retryCount((short) 2)
                .build()
        );

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(2, 0, 0, 0, 2, 0, 0, 0, 0);

    PendingBillRow pendingBill1 = getPendingBill("1000000000001");
    PendingBillRow pendingBill2 = getPendingBill("1000000000002");
    assertNotNull(pendingBill1.getFirstFailureOn());
    assertThat(pendingBill1.getRetryCount(), is((short) 1));
    assertThat(pendingBill2.getRetryCount(), is((short) 3));
  }

  /*
   * 1 Billable Item with Monthly Bill Cycle is kept as 1 Pending Bill.
   * Next day, another Billable Item is aggregated with the Pending Bill from yesterday.
   */
  @Test
  void when2BillableItemsWithMonthlyBillCycleAreProcessedIn2DifferentDaysThenTheyAreAggregatedInSamePendingBill() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_31);

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(1, 0, 1, 0, 0, 0, 0, 0, 0);

    PendingBillRow pendingBill = getPendingBill();
    assertPendingBillLineCount(pendingBill, 1);

    pendingBillSourceBuilder.addRows(removeBillLineDetails(getRows(pendingBillWriter)));
    standardBillableItemSourceBuilder.clear().addRows(changeBillableItemAccruedDate(BILLABLE_ITEM_32, DATE_2021_01_28));

    createDriverAndRun(DATE_2021_01_28, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(1, 0, 1, 0, 0, 0, 0, 0, 0);

    pendingBill = getPendingBill();
    assertPendingBillLineCount(pendingBill, 2);
  }

  /*
   * 1 Billable Item with Monthly Bill Cycle is kept as 1 Pending Bill.
   * Next day, the bill cycle of the account changes so the Pending Bill is completed.
   */
  @Test
  void whenBillCycleOfExistingPendingBillChangesFromMonthlyToDailyThenCompleteBill() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_31);

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(1, 0, 1, 0, 0, 0, 0, 0, 0);

    pendingBillSourceBuilder.addRows(removeBillLineDetails(getRows(pendingBillWriter)));
    standardBillableItemSourceBuilder.clear();
    inMemoryAccountRepository.clearAccounts();
    inMemoryAccountRepository.clearAccountDetails();
    inMemoryAccountRepository.addAccount(changeBillCycleCode(BILLING_ACCOUNT_3, WPDY));
    inMemoryAccountRepository.addAccountDetail(changeBillCycleCode(ACCOUNT_DETAILS_3, WPDY));

    createDriverAndRun(DATE_2021_01_28, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(0, 1, 0, 0, 0, 0, 0, 0, 2);

    BillRow completeBill = getCompleteBill();
    assertCompleteBillLineCount(completeBill, 1);
    assertThat(completeBill.getEndDate().toLocalDate(), equalTo(DATE_2021_01_28));
  }

  /*
   * 7.
   * Given a transaction where the Accrued_dt is <= than the billing run date and a bill cycle change from monthly to daily,
   * When billing runs,
   * Then a complete bill should be generated summing up the existing monthly pending bill with the latest transactions.
   */
  @Test
  void whenBillCycleOfExistingPendingBillChangesFromMonthlyToDailyAndNewBillableItemsAreConsumedThenCompleteBill() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_31);

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(1, 0, 1, 0, 0, 0, 0, 0, 0);

    pendingBillSourceBuilder.addRows(removeBillLineDetails(getRows(pendingBillWriter)));
    standardBillableItemSourceBuilder.clear().addRows(changeBillableItemAccruedDate(BILLABLE_ITEM_32, DATE_2021_01_28));
    inMemoryAccountRepository.clearAccounts();
    inMemoryAccountRepository.clearAccountDetails();
    inMemoryAccountRepository.addAccount(changeBillCycleCode(BILLING_ACCOUNT_3, WPDY));
    inMemoryAccountRepository.addAccountDetail(changeBillCycleCode(ACCOUNT_DETAILS_3, WPDY));

    createDriverAndRun(DATE_2021_01_28, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(0, 1, 1, 0, 0, 0, 0, 0, 4);

    BillRow completeBill = getCompleteBill();
    assertCompleteBillLineCount(completeBill, 2);
    assertThat(completeBill.getEndDate().toLocalDate(), equalTo(DATE_2021_01_28));
  }

  /*
   * 27.
   * Given there is no new transaction for a monthly pending bill,
   * When billing runs for the last day of the month,
   * Then a complete bill should be generated
   */
  @Test
  void whenPendingBillWithMonthlyBillCycleAndLogicalDateIsEndOfBillCycleThenCompleteBill() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_31);

    createDriverAndRun(DATE_2021_01_30, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(1, 0, 1, 0, 0, 0, 0, 0, 0);

    pendingBillSourceBuilder.addRows(removeBillLineDetails(getRows(pendingBillWriter)));
    standardBillableItemSourceBuilder.clear();

    createDriverAndRun(DATE_2021_01_31, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(0, 1, 0, 0, 0, 0, 0, 0, 2);
  }

  /*
   * 28.
   * 1 failed standard Billable Item,
   * 1 fixed standard billable
   * The fixed item is going to pending bill and an entry in BillItemError is added
   * 1 Fixed entry is also added in the BillItemError table
   */
  @Test
  void whenFailedBillableItemsAreSolvedThenFixedRecordIsWritten() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_52_FIXED);

    createDriverAndRun(DATE_2021_01_30, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0);
  }

  /*
   * 29.
   * 1 failed standard Billable Item,
   * 1 fixed standard billable
   * The fixed item is going to pending bill and an entry in BillItemError is added
   * 1 Fixed entry is also added in the BillItemError table
   */
  @Test
  void whenFixedAndFailedBillableItemThenFixedDateIsPresentInBillableItemErrorEntryAlongsideTheFailedEntry() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_52_FAILED, BILLABLE_ITEM_52_FIXED);

    createDriverAndRun(DATE_2021_01_30, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(1, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0);
  }

  /*
   * 30.
   * 1 failed standard Billable Item,
   * 1 failed misc Billable Item,
   * 1 fixed standard billable
   * 1 fixed misc billable
   * 2 items are going to pending Bill and 2 entries in BillItemError are added
   * 2 Fixed entries are also added in the BillItemError table
   */
  @Test
  void whenFixedAndFailedBillableItemsThenFixedDateIsPresentInBillableItemErrorEntriesAlongsideTheFailedEntries() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_52_FAILED, BILLABLE_ITEM_52_FIXED);
    miscBillableItemSourceBuilder.addRows(BILLABLE_ITEM_53_FAILED, BILLABLE_ITEM_53_FIXED);

    createDriverAndRun(DATE_2021_01_30, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(2, 0, 2, 2, 2, 0, 0, 1, 0, 0, 0);
  }

  /**
   * 31. Given some failed transactions from previous days, they are retried at every run and retry count is increased
   */
  @Test
  void whenFailedBillableItemsThenTheyAreRetriedAtEveryRun() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_11, BILLABLE_ITEM_52_FAILED);

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);
    assertOutputCounts(0, 1, 1, 1, 0, 0, 0, 0, 1);
    assertThatFailedItemFirstFailureAndRetryCountAre("2021-01-11", 23);

    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_52_FIXED);
    createDriverAndRun(DATE_2021_01_30, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);
    assertOutputCounts(1, 1, 2, 1, 1, 0, 0, 0, 0, 0, 1);
    assertThatFixedItemFirstFailureAndRetryCountAre("2021-01-27", 24);
  }

  /**
   * logical date = end of monthly bill cycle
   * <p>
   * 2 billable items with daily bill cycle, 1 with accrued date = 1 day prior to logical date and 1 with accrued date = logical date both
   * will be completed as 2 separate bills
   * <p>
   * 3 billable items with monthly bill cycle, 1 with accrued date 1 day prior to logical date and 2 with accrued date = logical date the
   * one from the previous day will be completed the 2 from today will be aggregated and saved as pending
   */
  @Test
  void whenBillingSkipsARunTheNextDayAlsoYesterdaysBillableItemsAreConsumed() {
    standardBillableItemSourceBuilder.addRows(
        // daily bill cycle billable items
        changeBillableItemAccruedDate(BILLABLE_ITEM_11, DATE_2021_01_31),
        changeBillableItemAccruedDate(BILLABLE_ITEM_12, DATE_2021_02_01),
        // monthly bill cycle billable items
        changeBillableItemAccruedDate(BILLABLE_ITEM_31, DATE_2021_01_31),
        changeBillableItemAccruedDate(BILLABLE_ITEM_32, DATE_2021_02_01),
        changeBillableItemAccruedDate(BILLABLE_ITEM_33, DATE_2021_02_01)
    );

    createDriverAndRun(DATE_2021_02_01, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(1, 3, 5, 0, 0, 0, 0, 0, 4);
  }

  /*
   * Bill cycle changes from daily to monthly
   */
  @Test
  void whenBillCycleChangesFromDailyToMonthlyThenTheNewBillableItemsAreProcessedWithMonthlyBillCycle() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_11);

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(0, 1, 1, 0, 0, 0, 0, 0, 1);

    standardBillableItemSourceBuilder.clear().addRows(changeBillableItemAccruedDate(BILLABLE_ITEM_12, DATE_2021_01_28));
    inMemoryAccountRepository.clearAccounts();
    inMemoryAccountRepository.clearAccountDetails();
    inMemoryAccountRepository.addAccount(changeBillCycleCode(BILLING_ACCOUNT_1, WPMO));
    inMemoryAccountRepository.addAccountDetail(changeBillCycleCode(ACCOUNT_DETAILS_1, WPMO));

    createDriverAndRun(DATE_2021_01_28, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(1, 0, 1, 0, 0, 0, 0, 0, 0);
  }

  @Test
  void whenChargesArePresentButCannotMatchAnyTaxRuleThenBillErrorIsGenerated() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_CHRG_11);

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(1, 0, 1, 0, 1, 0, 0, 0, 0);
    BillErrorDetail billErrorDetail = getBillError().getBillErrorDetails()[0];
    assertThat(billErrorDetail.getCode(), equalTo(ErrorCatalog.NO_TAX_RULE_FOUND));
  }

  @Test
  void whenChargesArePresentAndMatchATaxRuleButAPendingLineDoesNotMatchAnyTaxProductCharacteristicThenBillErrorIsGenerated() {
    inMemoryBillingRepository
        .addParty(PARTY_ROU_INT_EU_N)
        .addTaxRule(R_ROU_SST);
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_CHRG_12);

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_AND_ADHOC_BILLING);

    assertOutputCounts(1, 0, 1, 0, 1, 0, 0, 0, 0);
    BillErrorDetail billErrorDetail = getBillError().getBillErrorDetails()[0];
    assertThat(billErrorDetail.getCode(), equalTo(ErrorCatalog.NO_TAX_PRODUCT_CHARACTERISTIC_FOUND));
  }

  @Test
  void whenChargesArePresentButAPendingLineDoesNotMatchAnyTaxRatesThenBillErrorIsGenerated() {
    inMemoryBillingRepository
        .addParty(PARTY_ROU_INT_EU_N)
        .addTaxRule(R_ROU_SST)
        .addProductCharacteristic(FRAVI_TX_L_ROU);
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_CHRG_12);

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_AND_ADHOC_BILLING);

    assertOutputCounts(1, 0, 1, 0, 1, 0, 0, 0, 0);
    BillErrorDetail billErrorDetail = getBillError().getBillErrorDetails()[0];
    assertThat(billErrorDetail.getCode(), equalTo(ErrorCatalog.NO_TAX_RATE_FOUND));
  }

  @Test
  void whenChargesArePresentThenTaxIsComputed() {
    inMemoryBillingRepository
        .addParty(PARTY_ROU_INT_EU_N)
        .addTaxRule(R_ROU_SST)
        .addProductCharacteristic(FRAVI_TX_L_ROU)
        .addTaxRate(RATE_ROU_L);
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_CHRG_12);

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_AND_ADHOC_BILLING);

    assertOutputCounts(0, 1, 1, 0, 0, 2, 1, 0, 2);

    // bill amount contains tax
    BillRow billRow = getCompleteBill();
    assertThat(billRow.getBillAmount().compareTo(BigDecimal.valueOf(132.5)), is(0));
  }

  @Test
  void whenMinChargeSetUpMonthlyAndBillIsDailyAndHasChargesLessThanMinChargeThenPendingMinChargeApplies() {
    inMemoryBillingRepository
        .addParty(PARTY_ROU_INT_EU_N)
        .addTaxRule(R_ROU_SST)
        .addProductCharacteristic(FRAVI_TX_L_ROU)
        .addTaxRate(RATE_ROU_L)
        .addMinimumCharge(MINIMUM_CHARGE_1);
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_CHRG_21);
    pendingMinChargeSourceBuilder.addRows(PENDING_MINIMUM_CHARGE_1);

    createDriverAndRun(DATE_2021_01_27, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(0, 1, 1, 0, 0, 1, 1, 1, 2);

    BillRow bill = getCompleteBill();
    BillPriceRow billPrice = getBillPrice();

    assertThat(bill.getGranularity(), equalTo(BILLABLE_ITEM_CHRG_21.getGranularity()));
    assertThat(billPrice.getGranularity(), equalTo(BILLABLE_ITEM_CHRG_21.getGranularity()));
  }

  @Test
  void whenMinChargeSetUpMonthlyAndBillIsMonthlyAndHasChargesLessThanMinChargeThenMinChargeRemainderIsApplied() {
    inMemoryBillingRepository
        .addParty(PARTY_ROU_INT_EU_N)
        .addTaxRule(R_ROU_SST)
        .addProductCharacteristic(FRAVI_TX_L_ROU)
        .addProductCharacteristic(MINCHRGP_TX_L_ROU)
        .addTaxRate(RATE_ROU_L)
        .addMinimumCharge(MINIMUM_CHARGE_1);
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_CHRG_21);
    pendingMinChargeSourceBuilder.addRows(PENDING_MINIMUM_CHARGE_1);

    createDriverAndRun(DATE_2021_01_31, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    //Bill Price should have Tax + Min Charge
    assertOutputCounts(0, 1, 1, 0, 0, 2, 1, 0, 3);
    //Tax is applied on Base Charge + Min Charge
    assertThat(getBillTax().getBillLineTaxes().size(), is(2));
  }

  @Test
  void whenTaxIsCalculatedThenBillAccountingShouldContainAnEntryForEachBillTaxDetail() {
    inMemoryBillingRepository
        .addParty(PARTY_ROU_INT_EU_N)
        .addTaxRule(R_ROU_SST)
        .addProductCharacteristic(FRAVI_TX_L_ROU)
        .addProductCharacteristic(MINCHRGP_TX_L_ROU)
        .addTaxRate(RATE_ROU_L)
        .addMinimumCharge(MINIMUM_CHARGE_1);
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_CHRG_21);
    pendingMinChargeSourceBuilder.addRows(PENDING_MINIMUM_CHARGE_1);

    createDriverAndRun(DATE_2021_01_31, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(0, 1, 1, 0, 0, 2, 1, 0, 3);
    val billAccountingWithTax = getBillAccounting(bA -> bA.getBillClass().equals("TAX"));
    assertThat(billAccountingWithTax.size(), is(1));
    val amount = billAccountingWithTax.get(0).getCalculationLineAmount();
    assertThat(amount.compareTo(BigDecimal.valueOf(180)), is(0));
  }

  @Test
  void whenProcessBillableItemWith2LinesWithSameKeyThenAggregateTheLines() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_34);

    createDriverAndRun(DATE_2021_01_31, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(0, 1, 1, 0, 0, 0, 0, 0, 1);
  }

  @Test
  void whenMerchantAmountSignageHasDifferentValuesThenFundingAmountIsNegatedOrNot() {
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_31.toBuilder().merchantAmountSignage(-1).build(),
        BILLABLE_ITEM_32,
        BILLABLE_ITEM_33.toBuilder().merchantAmountSignage(-1).build(),
        BILLABLE_ITEM_34);

    createDriverAndRun(DATE_2021_01_31, EMPTY_PROCESSING_GROUP, STANDARD_AND_ADHOC_BILLING);

    BillRow bill = getCompleteBill();

    List<Integer> fundAmounts = Arrays.stream(bill.getBillLines()).map(line -> line.getFundingAmount().intValue())
        .collect(Collectors.toList());
    assertThat(125320, is(in(fundAmounts)));
    assertThat(-125320, is(in(fundAmounts)));
    assertThat(0, is(in(fundAmounts)));

    List<Integer> svcQtyAmounts = Arrays.stream(bill.getBillLines())
        .flatMap(line -> Stream.of(line.getBillLineServiceQuantities()))
        .filter(svcQty -> svcQty.getServiceQuantityCode().equals("F_M_AMT"))
        .map(svc -> svc.getServiceQuantity().intValue()).collect(Collectors.toList());

    assertThat(125320, is(in(svcQtyAmounts)));
    assertThat(-125320, is(in(svcQtyAmounts)));
  }

  @Test
  void whenMiscalculationDetectedThenBillGoesInError() {
    inMemoryBillingRepository
        .addParty(PARTY_ROU_INT_EU_N)
        .addTaxRule(R_ROU_SST)
        .addProductCharacteristic(FRAVI_TX_L_ROU)
        .addProductCharacteristic(MINCHRGP_TX_L_ROU)
        .addTaxRate(RATE_ROU_L)
        .addMinimumCharge(MINIMUM_CHARGE_1);
    standardBillableItemSourceBuilder.addRows(BILLABLE_ITEM_CHRG_22);
    pendingMinChargeSourceBuilder.addRows(PENDING_MINIMUM_CHARGE_1);

    createDriverAndRun(DATE_2021_01_31, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    assertOutputCounts(1, 0, 1, 0, 1, 0, 0, 0, 0);
  }

  @Test
  void whenBillCorrectionIsPresentThenNewBillIsCompleted() {
    billCorrectionSourceBuilder.addRows(new Tuple2<>(INPUT_BILL_CORRECTION_ROW_1, new BillLineRow[]{BILL_LINE_ROW_1}))
        .addRows(new Tuple2<>(INPUT_BILL_CORRECTION_ROW_2, new BillLineRow[]{BILL_LINE_ROW_2}))
        .addRows(new Tuple2<>(INPUT_BILL_CORRECTION_ROW_2.withBillId("bill_id_2_2"), null))
        .addRows(new Tuple2<>(InputBillCorrectionRow.builder().correctionEventId("12345678901").billId("bill_id_3").build(), null));
    billTaxSourceBuilder.addRows(BILL_TAX_1);

    createDriverAndRun(DATE_2021_01_31, EMPTY_PROCESSING_GROUP, STANDARD_BILLING);

    val bill1 = completeBillWriter.collectAsList().stream().filter(b -> b.getPreviousBillId().equals("bill_id_1")).findAny();
    assertTrue("Bill 1 should be computed", bill1.isPresent());
    val bill2 = completeBillWriter.collectAsList().stream().filter(b -> b.getPreviousBillId().equals("bill_id_2")).findAny();
    assertTrue("Bill 2 should be computed", bill2.isPresent());

    val completedBill1 = bill1.get();
    assertThat(completedBill1.getBillAmount().stripTrailingZeros(), is(BigDecimal.valueOf(-16.3).stripTrailingZeros()));
    assertThat(completedBill1.getBillLines()[0].getTotalAmount().stripTrailingZeros(), is(BigDecimal.valueOf(-10).stripTrailingZeros()));

    val completedBill2 = bill2.get();
    assertThat(completedBill2.getBillAmount().stripTrailingZeros(), is(BigDecimal.valueOf(-20).stripTrailingZeros()));
    assertThat(completedBill2.getBillLines()[0].getTotalAmount().stripTrailingZeros(), is(BigDecimal.valueOf(-20).stripTrailingZeros()));

    Stream.of(billTaxWriter.collectAsList().get(0).getBillTaxDetails()).forEach(
        billTaxDetail -> assertTrue("BillTax amounts should be negative", billTaxDetail.getTaxAmount().compareTo(BigDecimal.ZERO) <= 0));

    billAccountingWriter.collectAsList().forEach(billAccountingRow -> assertTrue("BillAccounting total line amount should be negative",
        billAccountingRow.getTotalLineAmount().compareTo(BigDecimal.ZERO) <= 0));

    val billErrorList = billErrorWriter.collectAsList();
    val billError3Optional = billErrorList.stream().filter(b -> b.getBillId().equals("bill_id_3")).findAny();
    assertTrue("Missing bill error", billError3Optional.isPresent());
    val billError3 = billError3Optional.get();
    val billErrorDetails = billError3.getBillErrorDetails();
    assertThat(billErrorDetails, is(notNullValue()));
    assertThat(billErrorDetails.length, is(1));
    assertThat(billErrorDetails[0].getMessage(), is("No bill found with id [bill_id_3] suitable for correction id [12345678901]"));

    val billError22Optional = billErrorList.stream().filter(b -> b.getBillId().equals("bill_id_2_2")).findAny();
    assertTrue("Missing bill error", billError22Optional.isPresent());
    val billError22 = billError22Optional.get();
    val billErrorDetails22 = billError22.getBillErrorDetails();
    assertThat(billErrorDetails22, is(notNullValue()));
    assertThat(billErrorDetails22.length, is(1));
    assertTrue("Invalid error message!",
        billErrorDetails22[0].getMessage().contains("Invalid correction for bill id [bill_id_2_2] and correction id " +
        "[12345678901] with error: [java.lang.NullPointerException"));

    assertOutputCounts(0, 2, 0, 0, 2, 1, 1, 0, 4);

    String[] oldBillIds = {"bill_id_1", "bill_id_2"};
    List<String> newBillIds = completeBillWriter.collectAsList().stream().map(BillRow::getBillId).collect(Collectors.toList());
    billRelationshipWriter.collectAsList().forEach(billRelationship -> {
      assertThat(billRelationship.getChildBillId(), is(in(newBillIds)));
      assertThat(billRelationship.getParentBillId(), is(in(oldBillIds)));
    });
  }

  private BillingBatchRunResult createDriverAndRun(LocalDate logicalDate, List<String> processingGroups, List<String> billingTypes) {

    BillingConfiguration billingConfiguration = new BillingConfiguration();
    Factory<AccountRepository> accountRepositoryFactory = new DummyFactory<>(inMemoryAccountRepository.build());
    Factory<AccountDeterminationService> accountDeterminationServiceFactory = ExecutorLocalFactory.of(
        new AccountDeterminationServiceFactory(accountRepositoryFactory, billingConfiguration.getBilling()));
    Factory<BillingRepository> billingRepositoryFactory = new DummyFactory<>(
        inMemoryBillingRepository.logicalDate(Date.valueOf(logicalDate)).build());
    Factory<BillCorrectionService> billCorrectionServiceFactory = new BillCorrectionServiceFactory();
    Factory<IdGenerator> idGeneratorFactory = new DummyFactory<>(inMemoryBillIdGenerator.build());

    Driver billingDriver = new Driver(
        sparkSession,
        id -> 1L,
        new ReaderRegistry(
            standardBillableItemSourceBuilder.build(),
            miscBillableItemSourceBuilder.build(),
            pendingBillSourceBuilder.build(),
            pendingMinChargeSourceBuilder.build(),
            billCorrectionSourceBuilder.build(),
            billTaxSourceBuilder.build()),
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
            minimumChargeBillWriter),
        new BillableItemProcessingServiceFactory(accountDeterminationServiceFactory),
        new BillProcessingServiceFactory(accountDeterminationServiceFactory, billingRepositoryFactory, logicalDate, processingGroups,
            billingTypes, billingConfiguration.getBilling()),
        billingRepositoryFactory,
        billingConfiguration.getDriverConfig(),
        billCorrectionServiceFactory, idGeneratorFactory);

    return billingDriver.run(getGenericBatch());
  }

  private static Batch getGenericBatch() {
    Watermark watermark = new Watermark(LocalDateTime.MIN, LocalDateTime.MAX);
    BatchId id = new BatchId(watermark, 1);
    LocalDateTime createdAt = LocalDateTime.now();

    return new Batch(id, null, createdAt, "", null);
  }

  private void assertOutputCounts(long pendingBillCount, long completeBillCount, long billLineDetailCount,
      long failedBillableItemCount, long billErrorCount,
      long billPriceCount, long billTaxCount, long pendingMinChargeCount, long billAccountingCount) {
    assertOutputCounts(pendingBillCount, completeBillCount, billLineDetailCount, failedBillableItemCount, 0, billErrorCount, 0,
        billPriceCount, billTaxCount, pendingMinChargeCount, billAccountingCount);
  }

  private void assertOutputCounts(long pendingBillCount, long completeBillCount, long billLineDetailCount,
      long failedBillableItemCount, long fixedBillableItemCount, long billErrorCount, long fixedBillErrorCount,
      long billPriceCount, long billTaxCount, long pendingMinChargeCount, long billAccountingCount) {
    assertThat(failedBillableItemWriter.count(), is(failedBillableItemCount));
    assertThat(fixedBillableItemWriter.count(), is(fixedBillableItemCount));
    assertThat(billErrorWriter.count(), is(billErrorCount));
    assertThat(fixedBillErrorWriter.count(), is(fixedBillErrorCount));
    assertThat(pendingBillWriter.count(), is(pendingBillCount));
    assertThat(completeBillWriter.count(), is(completeBillCount));
    assertThat(billLineDetailWriter.count(), is(billLineDetailCount));
    assertThat(billPriceWriter.count(), is(billPriceCount));
    assertThat(billTaxWriter.count(), is(billTaxCount));
    assertThat(pendingMinChargeWriter.count(), is(pendingMinChargeCount));
    assertThat(billAccountingWriter.count(), is(billAccountingCount));
    assertIdsLength();
  }

  private static void assertPendingBillLineCount(PendingBillRow pendingBill, int count) {
    assertThat(pendingBill.getPendingBillLines().length, is(count));
  }

  private static void assertPendingBillLineCalcCount(PendingBillRow pendingBill, String priceLineId, int count) {
    Optional<PendingBillLineRow> pendingBillLine = java.util.stream.Stream.of(pendingBill.getPendingBillLines())
        .filter(line -> priceLineId.equals(line.getPriceLineId()))
        .findAny();
    if (!pendingBillLine.isPresent()) {
      fail(String.format("Missing pending bill line with priceLineId = `%s`", priceLineId));
    }
    assertThat(pendingBillLine.get().getPendingBillLineCalculations().length, is(count));
  }

  private static void assertCompleteBillLineCount(BillRow billRow, int count) {
    assertThat(billRow.getBillLines().length, is(count));
  }

  private static PendingBillRow[] getRows(InMemoryWriter<PendingBillRow> writer) {
    return writer.collectAsList().toArray(new PendingBillRow[0]);
  }

  private void assertThatFailedItemFirstFailureAndRetryCountAre(String firstFailureOn, int retryCount) {
    assertThat(failedBillableItemWriter.collectAsList().get(0).getFirstFailureOn(), is(Date.valueOf(firstFailureOn)));
    assertThat(failedBillableItemWriter.collectAsList().get(0).getRetryCount(), is(retryCount));
  }

  private void assertThatFixedItemFirstFailureAndRetryCountAre(String firstFailureOn, int retryCount) {
    assertThat(fixedBillableItemWriter.collectAsList().get(0).getFirstFailureOn(), is(Date.valueOf(firstFailureOn)));
    assertThat(fixedBillableItemWriter.collectAsList().get(0).getRetryCount(), is(retryCount));
  }

  private PendingBillRow getPendingBill() {
    return pendingBillWriter.collectAsList().get(0);
  }

  private PendingBillRow getPendingBill(String billId) {
    return pendingBillWriter.collectAsList().stream()
        .filter(bill -> billId.equals(bill.getBillId()))
        .findAny()
        .orElseGet(() -> fail("Could not find pending bill"));
  }

  private BillRow getCompleteBill() {
    return completeBillWriter.collectAsList().get(0);
  }

  private BillRow getCompleteBill(String billId) {
    return completeBillWriter.collectAsList().stream()
        .filter(bill -> billId.equals(bill.getBillId()))
        .findAny()
        .orElseGet(() -> fail("Could not find bill"));
  }

  private BillLineDetailRow getBillLineDetail(String billableItemId) {
    return billLineDetailWriter.collectAsList().stream()
        .filter(billLineDetail -> billableItemId.equals(billLineDetail.getBillItemId()))
        .findAny()
        .orElseGet(() -> fail("Could not find bill line detail"));
  }

  private BillError getBillError() {
    return billErrorWriter.collectAsList().get(0);
  }

  private BillPriceRow getBillPrice() {
    return billPriceWriter.collectAsList().get(0)._2();
  }

  private BillPriceRow getBillPrice(String billableItemId) {
    return billPriceWriter.collectAsList().stream()
        .map(Tuple2::_2)
        .filter(billPrice -> billableItemId.equals(billPrice.getBillableItemId()))
        .findAny()
        .orElseGet(() -> fail("Could not find bill price for billableItemId=" + billableItemId));
  }

  private List<BillPriceRow> getBillPrices() {
    return billPriceWriter.collectAsList().stream()
        .map(Tuple2::_2)
        .collect(Collectors.toList());
  }

  private BillTax getBillTax() {
    return billTaxWriter.collectAsList().get(0);
  }

  private List<BillAccountingRow> getBillAccounting(Predicate<BillAccountingRow> filter) {
    return billAccountingWriter.collectAsList().stream()
        .filter(filter)
        .collect(Collectors.toList());
  }

  private void assertIdsLength() {
    for (PendingBillRow bill : pendingBillWriter.collectAsList()) {
      assertIdsLength(bill);
    }
    for (BillRow bill : completeBillWriter.collectAsList()) {
      assertIdsLength(bill);
    }
    for (BillTax billTax : billTaxWriter.collectAsList()) {
      assertIdLength(billTax, BillTax::getBillTaxId, 36);
    }
    for (Tuple2<String, BillPriceRow> billPriceRow : billPriceWriter.collectAsList()) {
      assertIdLength(billPriceRow._1(), Function.identity(), 36);
    }
  }

  private void assertIdsLength(PendingBillRow bill) {
    assertIdLength(bill, PendingBillRow::getBillId, 13);
    for (PendingBillLineRow line : bill.getPendingBillLines()) {
      assertIdLength(line, PendingBillLineRow::getBillLineId, 36);
      for (PendingBillLineCalculationRow calc : line.getPendingBillLineCalculations()) {
        assertIdLength(calc, PendingBillLineCalculationRow::getBillLineCalcId, 36);
      }
    }
  }

  private void assertIdsLength(BillRow bill) {
    assertIdLength(bill, BillRow::getBillId, 13);
    for (BillLineRow line : bill.getBillLines()) {
      assertIdLength(line, BillLineRow::getBillLineId, 36);
      for (BillLineCalculationRow calc : line.getBillLineCalculations()) {
        assertIdLength(calc, BillLineCalculationRow::getBillLineCalcId, 36);
      }
    }
  }

  private <T> void assertIdLength(T object, Function<T, String> idGetter, int length) {
    String id = idGetter.apply(object);
    assertThat(id, notNullValue());
    assertThat(id.length(), is(length));
  }
}
