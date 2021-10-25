package com.worldpay.pms.bse.domain.mincharge;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;

import com.worldpay.pms.bse.domain.DefaultRoundingService;
import com.worldpay.pms.bse.domain.RoundingService;
import com.worldpay.pms.bse.domain.account.BillingAccount;
import com.worldpay.pms.bse.domain.account.BillingCycle;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBill;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLine;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLineCalculation;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLineServiceQuantity;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumCharge;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumChargeKey;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumChargeResult;
import com.worldpay.pms.bse.domain.model.mincharge.PendingMinimumCharge;
import com.worldpay.pms.bse.domain.staticdata.Currency;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import io.vavr.collection.Stream;
import io.vavr.collection.Traversable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DefaultMinimumChargeCalculationServiceTest {

  private static final LocalDate LOGICAL_DATE = LocalDate.parse("2021-03-30");
  public final static Supplier<CompleteBill.CompleteBillBuilder> COMPLETE_BILL = () ->
      CompleteBill.builder()
          .billId("BILL_ID")
          .partyId("P00002")
          .legalCounterpartyId("P00001")
          .accountType("CHRG")
          .processingGroup("ROW")
          .currency("GBP")
          .scheduleEnd(LOGICAL_DATE)
          .billAmount(BigDecimal.valueOf(11))
          .billLines(new CompleteBillLine[]{
              buildCompleteBillLine("bill_line1", "P00005", "PREMIUM",
                  new CompleteBillLineCalculation[]{buildBillLineCalc("bill_line_calc1", BigDecimal.valueOf(10))}),
              buildCompleteBillLine("bill_line2", "P00006", "PREMIUM",
                  new CompleteBillLineCalculation[]{buildBillLineCalc("bill_line_calc1", BigDecimal.valueOf(1))})
          });
  public static final LocalDate MIN_CHARGE_START_DATE = LocalDate.parse("2021-03-01");
  public static final LocalDate MIN_CHARGE_END_DATE = LocalDate.parse("2021-03-31");

  public final static Supplier<BillingAccount.BillingAccountBuilder> CHARGING_ACCOUNT = () ->
      BillingAccount.builder()
          .billingCycle(new BillingCycle("WPDY", null, null))
          .accountId("1234")
          .currency("GBP")
          .accountType("CHRG")
          .businessUnit("BU123")
          .legalCounterparty("P00001")
          .childPartyId("P00008")
          .partyId("P00007")
          .subAccountType("CHRG")
          .subAccountId("567")
          .processingGroup("ROW");


  private final DefaultMinimumChargeCalculationService service = getService();

  private static RoundingService roundingService;

  @BeforeAll
  static void init() {
    roundingService = new DefaultRoundingService(List.of(
        new Currency("EUR", (short) 2),
        new Currency("GBP", (short) 2),
        new Currency("JPY", (short) 0)
    ));
  }

  @Test
  @DisplayName("When there is no minimum charge setup then return empty result")
  void whenNoPendingMinChargeThenCreateCorrectPendingMinCharge() {

    MinimumChargeResult result = service.compute(COMPLETE_BILL.get().partyId("P00007").build(), List.empty());
    assertThat(List.of(result.getCompleteBill().getBillLines()), iterableWithSize(2));
    assertThat(result.getPendingMinimumCharges(), iterableWithSize(0));
  }

  @Test
  @DisplayName("When minimum charge is set at the parent level and not complete then return correct pending min charges")
  void whenNoPendingMinChargeAndMinChargeSetupThenReturnCorrectResult() {

    MinimumChargeResult result = service.compute(COMPLETE_BILL.get().build(),
        List.of(buildPendingMinimumCharge("P00001", "P00002", "P00002", BigDecimal.ZERO, null)));
    assertThat(List.of(result.getCompleteBill().getBillLines()), iterableWithSize(2));
    assertThat(result.getPendingMinimumCharges(), iterableWithSize(1));

    assertThat(result.getPendingMinimumCharges(), hasItem(
        buildPendingMinimumCharge("P00001", "P00002", "P00002", BigDecimal.valueOf(11), MIN_CHARGE_END_DATE)));
  }

  @Test
  @DisplayName("When minimum charge is set at the child level and no pending min charges and should not complete then return pending min charge")
  void whenNoPendingMinChargeAndMinChargeSetupAtChildLevelThenReturnCorrectResult() {
    MinimumChargeResult result = service.compute(COMPLETE_BILL.get()
            .partyId("P00007")
            .billLines(new CompleteBillLine[]{
                buildCompleteBillLine("bill_line1", "P00003", "ACQUIRED", new CompleteBillLineCalculation[]{
                    buildBillLineCalc("bill_line_calc1", BigDecimal.valueOf(10)),
                    buildBillLineCalc("bill_line_calc2", BigDecimal.valueOf(5)),
                }),
                buildCompleteBillLine("bill_line2", "P00004", "ACQUIRED",
                    new CompleteBillLineCalculation[]{buildBillLineCalc("bill_line_calc1", BigDecimal.valueOf(10))}),
                buildCompleteBillLine("bill_line4", "P00004", "abc",
                    new CompleteBillLineCalculation[]{buildBillLineCalc("bill_line_calc1", BigDecimal.valueOf(10))}),
                buildCompleteBillLine("bill_line3", "P00002", "PREMIUM",
                    new CompleteBillLineCalculation[]{buildBillLineCalc("bill_line_calc1", BigDecimal.valueOf(20))})
            }).build(),
        List.of(buildPendingMinimumCharge("P00001", "P00007", "P00003", BigDecimal.ZERO, null),
            buildPendingMinimumCharge("P00001", "P00007", "P00004", BigDecimal.ZERO, null),
            buildPendingMinimumCharge("P00001", "P00007", "P00002", BigDecimal.ZERO, null)
        ));

    assertThat(List.of(result.getCompleteBill().getBillLines()), iterableWithSize(4));
    assertThat(result.getPendingMinimumCharges(), iterableWithSize(3));

    assertThat(result.getPendingMinimumCharges(), hasItem(
        buildPendingMinimumCharge("P00001", "P00007", "P00003", BigDecimal.valueOf(15), MIN_CHARGE_END_DATE)));
    assertThat(result.getPendingMinimumCharges(), hasItem(
        buildPendingMinimumCharge("P00001", "P00007", "P00004", BigDecimal.valueOf(10), MIN_CHARGE_END_DATE)));
    assertThat(result.getPendingMinimumCharges(), hasItem(
        buildPendingMinimumCharge("P00001", "P00007", "P00002", BigDecimal.valueOf(20), MIN_CHARGE_END_DATE)));
  }

  @Test
  @DisplayName("When minimum charge is set at the child level and we have pending minimum charges and should not complete then return pending min charge")
  void whenPendingMinChargeExistsAndMinChargeSetupAtChildLevelThenReturnCorrectResult() {
    MinimumChargeResult result = service.compute(COMPLETE_BILL.get()
            .partyId("P00007")
            .billLines(new CompleteBillLine[]{
                buildCompleteBillLine("bill_line1", "P00003", "ACQUIRED", new CompleteBillLineCalculation[]{
                    buildBillLineCalc("bill_line_calc1", BigDecimal.valueOf(1)),
                    buildBillLineCalc("bill_line_calc2", BigDecimal.valueOf(5)),
                }),
                buildCompleteBillLine("bill_line2", "P00004", "ACQUIRED",
                    new CompleteBillLineCalculation[]{buildBillLineCalc("bill_line_calc1", BigDecimal.valueOf(10))}),
                buildCompleteBillLine("bill_line3", "P00002", "ACQUIRED",
                    new CompleteBillLineCalculation[]{buildBillLineCalc("bill_line_calc1", BigDecimal.valueOf(20))})
            }).build(),
        List.of(
            buildPendingMinimumCharge("P00001", "P00007", "P00003", BigDecimal.valueOf(10), MIN_CHARGE_END_DATE),
            buildPendingMinimumCharge("P00001", "P00007", "P00004", BigDecimal.valueOf(10), MIN_CHARGE_END_DATE),
            buildPendingMinimumCharge("P00001", "P00007", "P00002", BigDecimal.valueOf(15), MIN_CHARGE_END_DATE)
        ));

    assertThat(List.of(result.getCompleteBill().getBillLines()), iterableWithSize(3));
    assertThat(result.getPendingMinimumCharges(), iterableWithSize(3));

    assertThat(result.getPendingMinimumCharges(), hasItem(
        buildPendingMinimumCharge("P00001", "P00007", "P00003", BigDecimal.valueOf(16), MIN_CHARGE_END_DATE)));
    assertThat(result.getPendingMinimumCharges(), hasItem(
        buildPendingMinimumCharge("P00001", "P00007", "P00004", BigDecimal.valueOf(20), MIN_CHARGE_END_DATE)));
    assertThat(result.getPendingMinimumCharges(), hasItem(
        buildPendingMinimumCharge("P00001", "P00007", "P00002", BigDecimal.valueOf(35), MIN_CHARGE_END_DATE)));
  }

  @Test
  @DisplayName("When minimum charge is set at the child level and we have pending minimum charges and should complete then return correct result")
  void whenMinimumChargeAtChildLevelAndShouldCompleteReturnCorrectResult() {
    MinimumChargeResult result = service.compute(COMPLETE_BILL.get()
            .partyId("P00007")
            .billAmount(BigDecimal.valueOf(16L))
            .billLines(new CompleteBillLine[]{
                buildCompleteBillLine("bill_line1", "P00003", "PREMIUM", new CompleteBillLineCalculation[]{
                    buildBillLineCalc("bill_line_calc1", BigDecimal.valueOf(1.01)),
                    buildBillLineCalc("bill_line_calc2", BigDecimal.valueOf(5.011)),
                }),
                buildCompleteBillLine("bill_line2", "P00008", "PREMIUM", new CompleteBillLineCalculation[]{
                    buildBillLineCalc("bill_line_calc1", BigDecimal.valueOf(10.116))})
            }).build(),
        List.of(
            buildPendingMinimumCharge("P00001", "P00007", "P00003", BigDecimal.valueOf(10.011), MIN_CHARGE_END_DATE),
            buildPendingMinimumCharge("P00001", "P00007", "P00008", BigDecimal.ZERO, null)
        ));

    assertThat(List.of(result.getCompleteBill().getBillLines()), iterableWithSize(3));
    assertThat(result.getPendingMinimumCharges(), iterableWithSize(1));

    assertThat(result.getPendingMinimumCharges(), hasItem(
        buildPendingMinimumCharge("P00001", "P00007", "P00003", BigDecimal.valueOf(16.032), MIN_CHARGE_END_DATE)));
    assertThat(setIds(result.getCompleteBill().getBillLines(), "id_min_chg"), hasItem(buildMinChargeBillLine(BigDecimal.valueOf(14.88))));
  }

  @Test
  @DisplayName("When minimum charge is set at the parent level and we have pending min charge and should complete then return correct result")
  void whenPendingMinChargeShouldCompleteThenReturnCorrectResult() {
    MinimumChargeResult result = service.compute(COMPLETE_BILL.get().partyId("P00008").build(),
        List.of(
            buildPendingMinimumCharge("P00001", "P00008", "P00008", BigDecimal.valueOf(9.995), LOGICAL_DATE)
        ));

    assertThat(List.of(result.getCompleteBill().getBillLines()), iterableWithSize(3));
    assertThat(result.getPendingMinimumCharges(), iterableWithSize(0));

    assertThat(setIds(result.getCompleteBill().getBillLines(), "id_min_chg"), hasItem(buildMinChargeBillLine(BigDecimal.valueOf(4.01))));
  }

  @Test
  @DisplayName("When we have pending minimum charges and should complete but the applicable charges are greater than min charge then return empty list")
  void whenMinimumChargeAndShouldCompleteAndApplicableChargesAreGreaterReturnCorrectResult() {
    MinimumChargeResult result = service.compute(COMPLETE_BILL.get()
            .partyId("P00007")
            .billLines(new CompleteBillLine[]{
                buildCompleteBillLine("bill_line1", "P00008", "PREMIUM", new CompleteBillLineCalculation[]{
                    buildBillLineCalc("bill_line_calc1", BigDecimal.valueOf(1)),
                    buildBillLineCalc("bill_line_calc2", BigDecimal.valueOf(5)),
                }),
                buildCompleteBillLine("bill_line2", "P00008", "PREMIUM", new CompleteBillLineCalculation[]{
                    buildBillLineCalc("bill_line_calc1", BigDecimal.valueOf(10))})
            }).build(),
        List.of(
            buildPendingMinimumCharge("P00001", "P00007", "P00008", BigDecimal.valueOf(10), LocalDate.parse("2021-03-30"))
        ));

    assertThat(List.of(result.getCompleteBill().getBillLines()), iterableWithSize(2));
    assertThat(result.getPendingMinimumCharges(), iterableWithSize(0));
  }

  @Test
  void whenShouldCompleteAnyPendingThenReturnCorrectValue() {
    Boolean result = service.shouldCompleteAnyPending(
        List.of(
            buildPendingMinimumCharge("P00001", "P00007", "P00008", BigDecimal.ZERO, null),
            buildPendingMinimumCharge("P00001", "P00007", "P00004", BigDecimal.ZERO, null)
        ));
    assertThat(result, is(true));

    result = service.shouldCompleteAnyPending(
        List.of(
            buildPendingMinimumCharge("P00001", "P00007", "P00004", BigDecimal.ZERO, null)
        ));
    assertThat(result, is(false));
  }

  @Test
  void whenCreateCompleteBillForAccountThenReturnCorrectResult() {
    MinimumChargeResult result = service.compute(CHARGING_ACCOUNT.get().build(),
        List.of(buildPendingMinimumCharge("P00001", "P00007", "P00008", BigDecimal.ZERO, LocalDate.parse("2021-03-30")),
            buildPendingMinimumCharge("P00001", "P00007", "P00004", BigDecimal.ZERO, null)
        ));
    CompleteBill completeBill = CompleteBill.builder()
        .billId("id_min_chg")
        .partyId("P00007")
        .legalCounterpartyId("P00001")
        .accountId("1234")
        .billSubAccountId("567")
        .accountType("CHRG")
        .businessUnit("BU123")
        .billCycleId("CTM")
        .scheduleStart(MIN_CHARGE_START_DATE)
        .scheduleEnd(LocalDate.parse("2021-03-30"))
        .currency("GBP")
        .processingGroup("ROW")
        .billAmount(BigDecimal.valueOf(25).setScale(2))
        .billReference("12342021-03-01NNNN")
        .adhocBillIndicator("N")
        .releaseReserveIndicator("N")
        .releaseWAFIndicator("N")
        .fastestPaymentRouteIndicator("N")
        .individualBillIndicator("N")
        .manualBillNarrative("N")
        .billLines(io.vavr.collection.List.of(buildMinChargeBillLine(BigDecimal.valueOf(25).setScale(2))).toJavaArray(CompleteBillLine[]::new))
        .build();

    assertThat(setIds(result.getCompleteBill(), "id_min_chg"), is(completeBill));
    assertThat(result.getPendingMinimumCharges(), iterableWithSize(0));
  }

  @Test
  void whenGetPendingMinChargeThenReturnCorrectResult() {
    Seq<PendingMinimumCharge> result = service.getPendingMinCharges(
        List.of(
            buildPendingMinimumCharge("P00001", "P00007", "P00003", BigDecimal.valueOf(10L), MIN_CHARGE_END_DATE),
            buildPendingMinimumCharge("P00001", "P00007", "P00008", BigDecimal.valueOf(0), null),
            buildPendingMinimumCharge("P00001", "P00007", "P00002", BigDecimal.valueOf(0), null)
        )
    );

    assertThat(result, iterableWithSize(1));

    assertThat(result, hasItem(
        buildPendingMinimumCharge("P00001", "P00007", "P00003", BigDecimal.valueOf(10L), MIN_CHARGE_END_DATE)));

  }

  @Test
  @DisplayName("When min charge should complete and we have negative charges then apply just the min charge")
  void whenMinimumChargeAndShouldCompleteAndApplicableChargesAreNegativeReturnCorrectResult() {
    MinimumChargeResult result = service.compute(COMPLETE_BILL.get()
            .partyId("P00007")
            .billLines(new CompleteBillLine[]{
                buildCompleteBillLine("bill_line1", "P00008", "PREMIUM", new CompleteBillLineCalculation[]{
                    buildBillLineCalc("bill_line_calc1", BigDecimal.valueOf(1)),
                    buildBillLineCalc("bill_line_calc2", BigDecimal.valueOf(-5)),
                }),
                buildCompleteBillLine("bill_line2", "P00008", "PREMIUM", new CompleteBillLineCalculation[]{
                    buildBillLineCalc("bill_line_calc1", BigDecimal.valueOf(-10))})
            }).build(),
        List.of(
            buildPendingMinimumCharge("P00001", "P00007", "P00008", BigDecimal.valueOf(10), LocalDate.parse("2021-03-30"))
        ));

    assertThat(List.of(result.getCompleteBill().getBillLines()), iterableWithSize(3));
    assertThat(result.getPendingMinimumCharges(), iterableWithSize(0));

    assertThat(setIds(result.getCompleteBill().getBillLines(), "id_min_chg"),
        hasItem(buildMinChargeBillLine(BigDecimal.valueOf(25.00).setScale(2))));
  }

  private PendingMinimumCharge buildPendingMinimumCharge(String lcp, String billPartyId, String txnPartyId, BigDecimal applicableCharges,
      LocalDate scheduleEnd) {
    return new PendingMinimumCharge(billPartyId, lcp, txnPartyId,
        MIN_CHARGE_START_DATE, scheduleEnd, "MIN_CHG", applicableCharges, MIN_CHARGE_END_DATE, "GBP");
  }

  private static DefaultMinimumChargeCalculationService getService() {
    MinimumChargeStore store = key ->
    {
      BillingCycle cycle = new BillingCycle("WPMO", MIN_CHARGE_START_DATE, MIN_CHARGE_END_DATE);
      return Stream.of(new MinimumCharge("price_assign1", "P00001", "P00002", "MIN_CHG",
              BigDecimal.valueOf(15), "GBP", cycle),
          new MinimumCharge("price_assign2", "P00001", "P00003", "MIN_CHG",
              BigDecimal.valueOf(20), "GBP", cycle),
          new MinimumCharge("price_assign3", "P00001", "P00004", "MIN_CHG",
              BigDecimal.valueOf(25), "GBP", cycle),
          new MinimumCharge("price_assign4", "P00001", "P00008", "MIN_CHG",
              BigDecimal.valueOf(25), "GBP", new BillingCycle("CTM", MIN_CHARGE_START_DATE, LocalDate.parse("2021-03-30")))
      )
          .groupBy(MinimumChargeKey::from)
          .mapValues(Traversable::single)
          .get(key);
    };

    return new DefaultMinimumChargeCalculationService(LOGICAL_DATE, store, roundingService);
  }

  private static CompleteBillLine buildCompleteBillLine(String lineId, String partyId, String productClass,
      CompleteBillLineCalculation[] lineCalcs) {
    return CompleteBillLine.builder()
        .billLineId(lineId)
        .billLinePartyId(partyId)
        .billLineCalculations(lineCalcs)
        .productClass(productClass)
        .build();
  }

  private static CompleteBillLineCalculation buildBillLineCalc(String id, BigDecimal amount) {
    return CompleteBillLineCalculation.builder()
        .billLineCalcId(id)
        .amount(amount)
        .build();
  }

  private static CompleteBillLine buildMinChargeBillLine(BigDecimal amount) {
    return CompleteBillLine.builder().billLinePartyId("P00008")
        .billLineId("id_min_chg")
        .productClass("MISCELLANEOUS")
        .productIdentifier("MINCHRGP")
        .pricingCurrency("GBP")
        .quantity(BigDecimal.ONE)
        .priceLineId("price_assign4")
        .totalAmount(amount)
        .billLineCalculations(new CompleteBillLineCalculation[]{
            CompleteBillLineCalculation.builder()
                .billLineCalcId("id_min_chg")
                .calculationLineClassification("BASE_CHG")
                .calculationLineType("TOTAL_BB")
                .amount(amount)
                .includeOnBill("Y")
                .rateType("MIN_CHG")
                .rateValue(BigDecimal.valueOf(25))
                .build()
        })
        .billLineServiceQuantities(
            List.of(
                CompleteBillLineServiceQuantity.builder().serviceQuantityTypeCode("TXN_VOL").serviceQuantityValue(BigDecimal.ONE).build())
                .toJavaArray(CompleteBillLineServiceQuantity[]::new)
        )
        .build();
  }

  private static CompleteBill setIds(CompleteBill bill, String id) {
    return bill
        .withBillId(id)
        .withBillLines(setIds(bill.getBillLines(), id).toJavaArray(CompleteBillLine[]::new));
  }

  private static List<CompleteBillLine> setIds(CompleteBillLine[] lines, String id) {
    return List.of(lines)
        .map(l -> l.withBillLineId(id)
            .withBillLineCalculations(
                List.of(l.getBillLineCalculations())
                    .map(c -> c.withBillLineCalcId(id)).toJavaArray(CompleteBillLineCalculation[]::new)
            )
        );
  }
}