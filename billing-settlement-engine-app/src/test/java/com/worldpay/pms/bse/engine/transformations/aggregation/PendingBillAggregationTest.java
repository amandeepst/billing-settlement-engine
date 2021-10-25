package com.worldpay.pms.bse.engine.transformations.aggregation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.transformations.model.billprice.BillPriceRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetail;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.aggregation.PendingBillAggKey;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.aggregation.PendingBillLineAggKey;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.aggregation.PendingBillLineCalculationAggKey;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.aggregation.PendingBillLineServiceQuantityAggKey;
import com.worldpay.pms.testing.junit.SparkContext;
import com.worldpay.pms.testing.stereotypes.WithSpark;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.var;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

@WithSpark
class PendingBillAggregationTest {

  private static final long PARTITION_ID = 0L;

  private static final PendingBillAggKey PENDING_BILL_1_KEY =
      pendingBillKey("accountId1", date("2021-01-11"), "adhocBillIndicator1", "settlementSubLevelType1", "settlementSubLevelValue1",
          "granularityKeyValue1", date("2020-01-12"), "debtMigrationType1", "overpaymentIndicator1", "releaseWAFIndicator1",
          "releaseReserveIndicator1", "fastestPaymentRouteIndicator1", "caseidentifier1", "individualBillIndicator1",
          "manualBillNarrative1");
  private static final PendingBillAggKey PENDING_BILL_2_KEY =
      pendingBillKey("accountId2", date("2021-01-21"), "adhocBillIndicator2", "settlementSubLevelType2", "settlementSubLevelValue2",
          "granularityKeyValue2", date("2020-01-22"), "debtMigrationType2", "overpaymentIndicator2", "releaseWAFIndicator2",
          "releaseReserveIndicator2", "fastestPaymentRouteIndicator2", "caseIdentifier2", "individualBillIndicator2",
          "manualBillNarrative2");

  private static final PendingBillLineAggKey PENDING_BILL_1_LINE_1_KEY =
      pendingBillLineKey("billLinePartyId11", "productIdentifier11", "pricingCurrency11", "fundingCurrency11", "transactionCurrency11",
          "priceLineId11", "merchantCode11");
  private static final PendingBillLineAggKey PENDING_BILL_1_LINE_2_KEY =
      pendingBillLineKey("billLinePartyId12", "productIdentifier12", "pricingCurrency12", "fundingCurrency12", "transactionCurrency12",
          "priceLineId12", "merchantCode12");
  private static final PendingBillLineAggKey PENDING_BILL_2_LINE_1_KEY =
      pendingBillLineKey("billLinePartyId21", "productIdentifier21", "pricingCurrency21", "fundingCurrency21", "transactionCurrency21",
          "priceLineId21", "merchantCode21");
  private static final PendingBillLineAggKey PENDING_BILL_2_LINE_2_KEY =
      pendingBillLineKey("billLinePartyId22", "productIdentifier22", "pricingCurrency22", "fundingCurrency22", "transactionCurrency22",
          "priceLineId22", "merchantCode22");

  private static final PendingBillLineCalculationAggKey PENDING_BILL_1_LINE_1_CALCULATION_1_KEY =
      pendingBillLineCalculationKey("calculationLineClassification111", "calculationLineType111", "Y", "rateType111");
  private static final PendingBillLineCalculationAggKey PENDING_BILL_1_LINE_1_CALCULATION_2_KEY =
      pendingBillLineCalculationKey("calculationLineClassification112", "calculationLineType112", "Y", "rateType112");
  private static final PendingBillLineCalculationAggKey PENDING_BILL_1_LINE_2_CALCULATION_1_KEY =
      pendingBillLineCalculationKey("calculationLineClassification121", "calculationLineType112", "Y", "rateType112");
  private static final PendingBillLineCalculationAggKey PENDING_BILL_2_LINE_1_CALCULATION_1_KEY =
      pendingBillLineCalculationKey("calculationLineClassification211", "calculationLineType211", "Y", "rateType211");
  private static final PendingBillLineCalculationAggKey PENDING_BILL_2_LINE_2_CALCULATION_1_KEY =
      pendingBillLineCalculationKey("calculationLineClassification221", "calculationLineType221", "Y", "rateType221");

  private static final PendingBillLineServiceQuantityAggKey PENDING_BILL_1_LINE_1_SERVICE_QUANTITY_1_KEY =
      pendingBillLineServiceQuantityKey("serviceQuantityTypeCode111");
  private static final PendingBillLineServiceQuantityAggKey PENDING_BILL_1_LINE_1_SERVICE_QUANTITY_2_KEY =
      pendingBillLineServiceQuantityKey("serviceQuantityTypeCode112");
  private static final PendingBillLineServiceQuantityAggKey PENDING_BILL_1_LINE_2_SERVICE_QUANTITY_1_KEY =
      pendingBillLineServiceQuantityKey("serviceQuantityTypeCode121");
  private static final PendingBillLineServiceQuantityAggKey PENDING_BILL_2_LINE_1_SERVICE_QUANTITY_1_KEY =
      pendingBillLineServiceQuantityKey("serviceQuantityTypeCode211");
  private static final PendingBillLineServiceQuantityAggKey PENDING_BILL_2_LINE_2_SERVICE_QUANTITY_1_KEY =
      pendingBillLineServiceQuantityKey("serviceQuantityTypeCode221");

  @AfterAll
  static void afterAll() {
    SparkContext.reset();
  }

  @Test
  void whenDatasetIsEmptyThenResultIsEmpty() {
    var result = PendingBillAggregation.aggregate(SparkContext.emptyDataset(Encodings.PENDING_BILL_ENCODER));
    assertThat(result.count(), is(PARTITION_ID));
  }

  @Test
  void whenAggregating2PendingBillsThatBothHaveIdSetThenThrowException() {
    try {
      PendingBillAggregation.aggregate(datasetOf(
          pendingBillRow(PENDING_BILL_1_KEY, "bill1", "partyId1", "legalCounterpartyId1", "billSubAccountId1", "accountType1",
              "businessUnit1", "billCycleId1", date("2021-01-10"), "currency1", "billReference1", "granularity1", "N", PARTITION_ID,
              array(), array()
          ),
          pendingBillRow(PENDING_BILL_1_KEY, "bill1Bis", "partyId1Bis", "legalCounterpartyId1Bis", "billSubAccountId1Bis",
              "accountType1Bis",
              "businessUnit1Bis", "billCycleId1Bis", date("2021-01-09"), "currency1Bis", "billReference1Bis", "granularity1Bis", "N",
              PARTITION_ID,
              array(), array()
          )
      )).count();
    } catch (Exception e) {
      assertSparkThrows(e, PendingBillAggregationException.class, "Cannot merge 2 pending bills with non null ids: `bill1` and `bill1Bis`");
    }
  }

  @Test
  void whenAggregating2PendingBillLinesThatBothHaveIdSetThenThrowException() {
    try {
      PendingBillAggregation.aggregate(datasetOf(
          pendingBillRow(PENDING_BILL_1_KEY, "bill1", "partyId1", "legalCounterpartyId1", "billSubAccountId1", "accountType1",
              "businessUnit1", "billCycleId1", date("2021-01-10"), "currency1", "billReference1", "granularity1", "N", PARTITION_ID,
              array(pendingBillLineRow(PENDING_BILL_1_LINE_1_KEY, "billLine11", "productClass11", bigDecimal(10), bigDecimal(10),
                  bigDecimal(10), PARTITION_ID,
                  array(),  array(), array())),
              array()
          ),
          pendingBillRow(PENDING_BILL_1_KEY, null, "partyId1Bis", "legalCounterpartyId1Bis", "billSubAccountId1Bis", "accountType1Bis",
              "businessUnit1Bis", "billCycleId1Bis", date("2021-01-09"), "currency1Bis", "billReference1Bis", "granularity1Bis", "N",
              PARTITION_ID,
              array(pendingBillLineRow(PENDING_BILL_1_LINE_1_KEY, "billLine11Bis", "productClass11Bis", bigDecimal(11), bigDecimal(11),
                  bigDecimal(11), PARTITION_ID,
                  array(), array(), array())),
              array()
          )
      )).count();
    } catch (Exception e) {
      assertSparkThrows(e, PendingBillAggregationException.class,
          "Cannot merge 2 pending bill lines with non null ids: `billLine11` and `billLine11Bis`");
    }
  }

  @Test
  void whenAggregating2PendingBillLineCalculationsThatBothHaveIdSetThenThrowException() {
    try {
      PendingBillAggregation.aggregate(datasetOf(
          pendingBillRow(PENDING_BILL_1_KEY, "bill1", "partyId1", "legalCounterpartyId1", "billSubAccountId1", "accountType1",
              "businessUnit1", "billCycleId1", date("2021-01-10"), "currency1", "billReference1", "granularity1", "N", PARTITION_ID,
              array(pendingBillLineRow(PENDING_BILL_1_LINE_1_KEY, "billLine11", "productClass11", bigDecimal(10), bigDecimal(10),
                  bigDecimal(10), PARTITION_ID,
                  array(),
                  array(pendingBillLineCalculationRow(PENDING_BILL_1_LINE_1_CALCULATION_1_KEY, "billLineCalc111", bigDecimal(10),
                      bigDecimal(10), PARTITION_ID)),
                  array())), array()
          ),
          pendingBillRow(PENDING_BILL_1_KEY, null, "partyId1Bis", "legalCounterpartyId1Bis", "billSubAccountId1Bis", "accountType1Bis",
              "businessUnit1Bis", "billCycleId1Bis", date("2021-01-09"), "currency1Bis", "billReference1Bis", "granularity1Bis", "N",
              PARTITION_ID,
              array(pendingBillLineRow(PENDING_BILL_1_LINE_1_KEY, null, "productClass11Bis", bigDecimal(11), bigDecimal(11), bigDecimal(11),
                  PARTITION_ID,
                  array(),
                  array(pendingBillLineCalculationRow(PENDING_BILL_1_LINE_1_CALCULATION_1_KEY, "billLineCalc111Bis", bigDecimal(11),
                      bigDecimal(11), PARTITION_ID)),
                  array())), array()
          )
      )).count();
    } catch (Exception e) {
      assertSparkThrows(e, PendingBillAggregationException.class,
          "Cannot merge 2 pending bill line calculations with non null ids: `billLineCalc111` and `billLineCalc111Bis`");
    }
  }

  @Test
  void whenDatasetIsNotEmptyThenAggregationIsDoneCorrectly() {
    var result = PendingBillAggregation.aggregate(datasetOf(
        pendingBillRow(PENDING_BILL_1_KEY, "bill1", "partyId1", "legalCounterpartyId1", "billSubAccountId1", "accountType1",
            "businessUnit1", "billCycleId1", date("2021-01-10"), "currency1", "billReference1", "granularity1", "N", PARTITION_ID,
            array(
                pendingBillLineRow(PENDING_BILL_1_LINE_1_KEY, "billLine11", "productClass11", bigDecimal(10), bigDecimal(10),
                    bigDecimal(10), PARTITION_ID, BillLineDetail.array("billItemId1", "12345"),
                    array(
                        pendingBillLineCalculationRow(PENDING_BILL_1_LINE_1_CALCULATION_1_KEY, "billLineCalc111", bigDecimal(10),
                            bigDecimal(10), PARTITION_ID),
                        pendingBillLineCalculationRow(PENDING_BILL_1_LINE_1_CALCULATION_2_KEY, "billLineCalc112", bigDecimal(10),
                            bigDecimal(10), PARTITION_ID)),
                    array(
                        pendingBillLineServiceQuantityRow(PENDING_BILL_1_LINE_1_SERVICE_QUANTITY_1_KEY, bigDecimal(10), PARTITION_ID),
                        pendingBillLineServiceQuantityRow(PENDING_BILL_1_LINE_1_SERVICE_QUANTITY_2_KEY, bigDecimal(10), PARTITION_ID))),
                pendingBillLineRow(PENDING_BILL_1_LINE_2_KEY, "billLine12", "productClass12", bigDecimal(11), bigDecimal(11),
                    bigDecimal(11), PARTITION_ID, BillLineDetail.array("billItemId2", "54321"),
                    array(
                        pendingBillLineCalculationRow(PENDING_BILL_1_LINE_2_CALCULATION_1_KEY, "billLineCalc121", bigDecimal(11),
                            bigDecimal(11), PARTITION_ID)),
                    array(
                        pendingBillLineServiceQuantityRow(PENDING_BILL_1_LINE_2_SERVICE_QUANTITY_1_KEY, bigDecimal(11), PARTITION_ID)))),
            array()
        ),
        pendingBillRow(PENDING_BILL_1_KEY, null, "partyId1Bis", "legalCounterpartyId1Bis", "billSubAccountId1Bis", "accountType1Bis",
            "businessUnit1Bis", "billCycleId1Bis", date("2021-01-09"), "currency1Bis", "billReference1Bis", "granularity1Bis", "Y",
            PARTITION_ID,
            array(
                pendingBillLineRow(PENDING_BILL_1_LINE_1_KEY, null, "productClass11Bis", bigDecimal(12), bigDecimal(12), bigDecimal(12),
                    PARTITION_ID, BillLineDetail.array("billItemId3", "12245"),
                    array(
                        pendingBillLineCalculationRow(PENDING_BILL_1_LINE_1_CALCULATION_1_KEY, null, bigDecimal(12), bigDecimal(12),
                            PARTITION_ID)),
                    array(
                        pendingBillLineServiceQuantityRow(PENDING_BILL_1_LINE_1_SERVICE_QUANTITY_1_KEY, bigDecimal(12), PARTITION_ID)))),
            array(billPriceRow("partyId1Bis", "accountType1Bis", "productId", "productClass11Bis", bigDecimal(12),
                "currency1Bis", "billReference1Bis", "billItemId3", date("2021-01-09"), "granularity"))
        ),
        pendingBillRow(PENDING_BILL_2_KEY, "bill2", "partyId2", "legalCounterpartyId2", "billSubAccountId2", "accountType2",
            "businessUnit2", "billCycleId2", date("2021-01-20"), "currency2", "billReference2", "granularity2", "N", PARTITION_ID,
            array(
                pendingBillLineRow(PENDING_BILL_2_LINE_1_KEY, "billLine21", "productClass21", bigDecimal(20), bigDecimal(20),
                    bigDecimal(20), PARTITION_ID, array(),
                    array(
                        pendingBillLineCalculationRow(PENDING_BILL_2_LINE_1_CALCULATION_1_KEY, "billLineCalc211", bigDecimal(20),
                            bigDecimal(20), PARTITION_ID)),
                    array(
                        pendingBillLineServiceQuantityRow(PENDING_BILL_2_LINE_1_SERVICE_QUANTITY_1_KEY, bigDecimal(20), PARTITION_ID))),
                pendingBillLineRow(PENDING_BILL_2_LINE_2_KEY, "billLine22", "productClass22", bigDecimal(21), bigDecimal(21),
                    bigDecimal(21), PARTITION_ID, array(),
                    array(
                        pendingBillLineCalculationRow(PENDING_BILL_2_LINE_2_CALCULATION_1_KEY, "billLineCalc221", bigDecimal(21),
                            bigDecimal(21), PARTITION_ID)),
                    array(
                        pendingBillLineServiceQuantityRow(PENDING_BILL_2_LINE_2_SERVICE_QUANTITY_1_KEY, bigDecimal(21), PARTITION_ID)))),
            array()
        )
    ));

    List<PendingBillRow> resultAsList = result.collectAsList();

    assertThat(resultAsList.size(), is(2));

    Optional<PendingBillRow> pendingBill1 = resultAsList.stream().filter(pendingBill -> "bill1".equals(pendingBill.getBillId()))
        .findFirst();
    assertThat(pendingBill1.isPresent(), is(true));
    assertThat(pendingBill1.get().getPendingBillLines().length, is(2));
    assertThat(pendingBill1.get().getMiscalculationFlag(), is("Y"));
    Optional<BillPriceRow> billPrice = Arrays
        .stream(pendingBill1.get().getBillPrices())
        .findFirst();
    assertThat(billPrice.isPresent(), is(true));
    assertThat(billPrice.get().getAmount().compareTo(bigDecimal(12)), equalTo(0));
    Optional<PendingBillLineRow> pendingBillLine1 = Arrays.stream(pendingBill1.get().getPendingBillLines())
        .filter(pendingBillLine -> "billLine11".equals(pendingBillLine.getBillLineId()))
        .findFirst();
    assertThat(pendingBillLine1.isPresent(), is(true));
    assertThat(pendingBillLine1.get().getFundingAmount().compareTo(bigDecimal(22)), equalTo(0));
    assertThat(pendingBillLine1.get().getBillLineDetails().length, is(2));
    assertThat(getBillLineDetail(pendingBillLine1.get(), "billItemId1").getBillItemHash(), is("12345"));
    assertThat(getBillLineDetail(pendingBillLine1.get(), "billItemId3").getBillItemHash(), is("12245"));
    Optional<PendingBillLineCalculationRow> pendingBillLineCalculation1 = Arrays
        .stream(pendingBillLine1.get().getPendingBillLineCalculations())
        .filter(pendingBillLineCalculation -> "billLineCalc111".equals(pendingBillLineCalculation.getBillLineCalcId()))
        .findFirst();
    assertThat(pendingBillLineCalculation1.isPresent(), is(true));
    assertThat(pendingBillLineCalculation1.get().getAmount().compareTo(bigDecimal(22)), equalTo(0));
    Optional<PendingBillLineServiceQuantityRow> pendingBillLineServiceQuantity1 = Arrays
        .stream(pendingBillLine1.get().getPendingBillLineServiceQuantities())
        .filter(pendingBillLineServiceQuantity -> "serviceQuantityTypeCode111"
            .equals(pendingBillLineServiceQuantity.getServiceQuantityTypeCode()))
        .findFirst();
    assertThat(pendingBillLineServiceQuantity1.isPresent(), is(true));
    assertThat(pendingBillLineServiceQuantity1.get().getServiceQuantityValue().compareTo(bigDecimal(22)), equalTo(0));

    Optional<PendingBillRow> pendingBill2 = resultAsList.stream().filter(pendingBill -> "bill2".equals(pendingBill.getBillId()))
        .findFirst();
    assertThat(pendingBill2.isPresent(), is(true));
  }

  @Test
  void whenAggregating2PendingBillsThenResultShouldHaveNonKeyFieldValuesFromEntryWithIdNonNull() {
    List<PendingBillRow> resultAsList = PendingBillAggregation.aggregate(datasetOf(
        pendingBillRow(PENDING_BILL_1_KEY, null, "partyId1", "legalCounterpartyId1", "billSubAccountId1", "accountType1",
            "businessUnit1", "billCycleId1", date("2021-01-10"), "currency1", "billReference1", "granularity1", "N", PARTITION_ID,
            array(), array()
        ),
        pendingBillRow(PENDING_BILL_1_KEY, "bill1Bis", "partyId1Bis", "legalCounterpartyId1Bis", "billSubAccountId1Bis", "accountType1Bis",
            "businessUnit1Bis", "billCycleId1Bis", date("2021-01-09"), "currency1Bis", "billReference1Bis", "granularity1Bis", "Y",
            PARTITION_ID,
            array(), array()
        )
    )).collectAsList();

    assertThat(resultAsList.size(), is(1));

    Optional<PendingBillRow> pendingBill1 = resultAsList.stream().filter(pendingBill -> "bill1Bis".equals(pendingBill.getBillId()))
        .findFirst();
    assertThat(pendingBill1.isPresent(), is(true));
    assertThat(pendingBill1.get().getBillSubAccountId(), equalTo("billSubAccountId1Bis"));
    assertThat(pendingBill1.get().getScheduleStart(), equalTo(date("2021-01-09")));
    assertThat(pendingBill1.get().getScheduleEnd(), equalTo(date("2021-01-11")));
    assertThat(pendingBill1.get().getBillReference(), equalTo("billReference1Bis"));
    assertThat(pendingBill1.get().getMiscalculationFlag(), equalTo("Y"));
  }

  @Test
  void whenAggregating2PendingBillsWithMiscalculationFlagYesAndSameKeyThenAggregateCorrect() {
    List<PendingBillRow> resultAsList = PendingBillAggregation.aggregate(datasetOf(
        pendingBillRow(PENDING_BILL_1_KEY, null, "partyId1", "legalCounterpartyId1", "billSubAccountId1", "accountType1",
            "businessUnit1", "billCycleId1", date("2021-01-10"), "currency1", "billReference1", "granularity1", "Y", PARTITION_ID,
            array(), array()
        ),
        pendingBillRow(PENDING_BILL_1_KEY, "bill1Bis", "partyId1Bis", "legalCounterpartyId1Bis", "billSubAccountId1Bis", "accountType1Bis",
            "businessUnit1Bis", "billCycleId1Bis", date("2021-01-09"), "currency1Bis", "billReference1Bis", "granularity1Bis", "Y",
            PARTITION_ID,
            array(), array()
        )
    )).collectAsList();

    assertThat(resultAsList.size(), is(1));

    Optional<PendingBillRow> pendingBill1 = resultAsList.stream().filter(pendingBill -> "bill1Bis".equals(pendingBill.getBillId()))
        .findFirst();
    assertThat(pendingBill1.get().getMiscalculationFlag(), equalTo("Y"));
  }

  private static PendingBillAggKey pendingBillKey(String accountId,
      Date scheduleEnd,
      String adhocBillIndicator,
      String settlementSubLevelType,
      String settlementSubLevelValue,
      String granularityKeyValue,
      Date debtDate,
      String debtMigrationType,
      String overpaymentIndicator,
      String releaseWAFIndicator,
      String releaseReserveIndicator,
      String fastestPaymentRouteIndicator,
      String caseIdentifier,
      String individualBillIndicator,
      String manualBillNarrative) {
    return new PendingBillAggKey(
        accountId,
        scheduleEnd,
        adhocBillIndicator,
        settlementSubLevelType,
        settlementSubLevelValue,
        granularityKeyValue,
        debtDate,
        debtMigrationType,
        overpaymentIndicator,
        releaseWAFIndicator,
        releaseReserveIndicator,
        fastestPaymentRouteIndicator,
        caseIdentifier,
        individualBillIndicator,
        manualBillNarrative
    );
  }

  private static PendingBillRow pendingBillRow(PendingBillAggKey key,
      String billId,
      String partyId,
      String legalCounterpartyId,
      String billSubAccountId,
      String accountType,
      String businessUnit,
      String billCycleId,
      Date scheduleStart,
      String currency,
      String billReference,
      String granularity,
      String miscalculationFlag,
      long partitionId,
      PendingBillLineRow[] pendingBillLines,
      BillPriceRow[] billPrices) {
    return new PendingBillRow(
        billId,
        partyId,
        legalCounterpartyId,
        key.getAccountId(),
        billSubAccountId,
        accountType,
        businessUnit,
        billCycleId,
        scheduleStart,
        key.getScheduleEnd(),
        currency,
        billReference,
        key.getAdhocBillIndicator(),
        key.getSettlementSubLevelType(),
        key.getSettlementSubLevelValue(),
        granularity,
        key.getGranularityKeyValue(),
        key.getDebtDate(),
        key.getDebtMigrationType(),
        key.getOverpaymentIndicator(),
        key.getReleaseWAFIndicator(),
        key.getReleaseReserveIndicator(),
        key.getFastestPaymentRouteIndicator(),
        key.getCaseIdentifier(),
        key.getIndividualBillIndicator(),
        key.getManualBillNarrative(),
        miscalculationFlag,
        null,
        (short) 0,
        partitionId,
        pendingBillLines,
        billPrices,
        null,
        null,
        null,
        0
    );
  }

  private static PendingBillLineAggKey pendingBillLineKey(String billLinePartyId,
      String productIdentifier,
      String pricingCurrency,
      String fundingCurrency,
      String transactionCurrency,
      String priceLineId,
      String merchantCode) {
    return new PendingBillLineAggKey(
        billLinePartyId,
        productIdentifier,
        pricingCurrency,
        fundingCurrency,
        transactionCurrency,
        priceLineId,
        merchantCode
    );
  }

  private static PendingBillLineRow pendingBillLineRow(PendingBillLineAggKey key,
      String billLineId,
      String productClass,
      BigDecimal fundingAmount,
      BigDecimal transactionAmount,
      BigDecimal quantity,
      long partitionId,
      BillLineDetail[] billLineDetails,
      PendingBillLineCalculationRow[] pendingBillLineCalculations,
      PendingBillLineServiceQuantityRow[] pendingBillLineServiceQuantities) {
    return new PendingBillLineRow(
        billLineId,
        key.getBillLinePartyId(),
        productClass,
        key.getProductIdentifier(),
        key.getPricingCurrency(),
        key.getFundingCurrency(),
        fundingAmount,
        key.getTransactionCurrency(),
        transactionAmount,
        quantity,
        key.getPriceLineId(),
        key.getMerchantCode(),
        partitionId,
        billLineDetails,
        pendingBillLineCalculations,
        pendingBillLineServiceQuantities
    );
  }

  private static PendingBillLineCalculationAggKey pendingBillLineCalculationKey(String calculationLineClassification,
      String calculationLineType,
      String includeOnBill,
      String rateType) {
    return new PendingBillLineCalculationAggKey(
        calculationLineClassification,
        calculationLineType,
        includeOnBill,
        rateType
    );
  }

  private static PendingBillLineCalculationRow pendingBillLineCalculationRow(PendingBillLineCalculationAggKey key,
      String billLineCalcId,
      BigDecimal amount,
      BigDecimal rateValue,
      long partitionId) {
    return new PendingBillLineCalculationRow(
        billLineCalcId,
        key.getCalculationLineClassification(),
        key.getCalculationLineType(),
        amount,
        key.getIncludeOnBill(),
        key.getRateType(),
        rateValue,
        partitionId
    );
  }

  private static PendingBillLineServiceQuantityAggKey pendingBillLineServiceQuantityKey(String serviceQuantityTypeCode) {
    return new PendingBillLineServiceQuantityAggKey(serviceQuantityTypeCode);
  }

  private static PendingBillLineServiceQuantityRow pendingBillLineServiceQuantityRow(PendingBillLineServiceQuantityAggKey key,
      BigDecimal serviceQuantityValue, long partitionId) {
    return new PendingBillLineServiceQuantityRow(key.getServiceQuantityTypeCode(), serviceQuantityValue, partitionId);
  }

  private static BillPriceRow billPriceRow(String partyId, String accountType, String productId, String calcLineType, BigDecimal amount,
      String currency, String billRef, String billItemId, Date accruedDate, String granularity) {
    return new BillPriceRow(partyId, accountType, productId, calcLineType, amount, currency, billRef, "Y", null,
        null, null, billItemId, null, accruedDate, granularity);
  }

  private static BillLineDetail getBillLineDetail(PendingBillLineRow pendingBillLine, String billItemId) {
    return Stream.of(pendingBillLine.getBillLineDetails())
        .filter(billLineDetail -> billItemId.equals(billLineDetail.getBillItemId()))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Bill line detail not found for item id = " + billItemId));
  }

  private void assertSparkThrows(Exception sparkException, Class<? extends Exception> exceptionClass, String exceptionMessage) {
    assertThat(sparkException.getClass(), is(SparkException.class));
    assertThat(sparkException.getCause().getClass(), is(exceptionClass));
    assertThat(sparkException.getCause().getMessage(), equalTo(exceptionMessage));
  }

  private static <T> T[] array(T... objects) {
    return objects;
  }

  private static Date date(String date) {
    return Date.valueOf(date);
  }

  private static BigDecimal bigDecimal(double value) {
    return BigDecimal.valueOf(value);
  }

  private static Dataset<PendingBillRow> datasetOf(PendingBillRow... rows) {
    return SparkContext.datasetOf(Encodings.PENDING_BILL_ENCODER, rows).coalesce(1);
  }
}