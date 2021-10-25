package com.worldpay.pms.bse.domain;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.nullValue;

import com.worldpay.pms.bse.domain.account.AccountDeterminationService;
import com.worldpay.pms.bse.domain.account.BillingAccount;
import com.worldpay.pms.bse.domain.account.BillingCycle;
import com.worldpay.pms.bse.domain.common.BillLineDomainError;
import com.worldpay.pms.bse.domain.mincharge.MinimumChargeCalculationService;
import com.worldpay.pms.bse.domain.model.PendingBill;
import com.worldpay.pms.bse.domain.model.TestPendingBill;
import com.worldpay.pms.bse.domain.model.TestPendingBillLine;
import com.worldpay.pms.bse.domain.model.TestPendingBillLineCalculation;
import com.worldpay.pms.bse.domain.model.TestPendingBillLineServiceQuantity;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTaxDetail;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBill;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillDecision;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLine;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLineCalculation;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLineServiceQuantity;
import com.worldpay.pms.bse.domain.model.completebill.UpdateBillDecision;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumChargeResult;
import com.worldpay.pms.bse.domain.model.mincharge.PendingMinimumCharge;
import com.worldpay.pms.bse.domain.tax.TaxCalculationService;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.Function2;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DefaultBillProcessingServiceTest {

  private static final Function<String, Validation<DomainError, String>> THROW_EXCEPTION_1 = x -> {
    throw new UnsupportedOperationException("Not implemented");
  };
  private static final Function2<String, LocalDate, Validation<DomainError, BillingCycle>> THROW_EXCEPTION_2 = (x, y) -> {
    throw new UnsupportedOperationException("Not implemented");
  };

  private static final String ACCOUNT_ID = "0123456789";

  private static final LocalDate DATE_2021_02_01 = LocalDate.parse("2021-02-01");
  private static final LocalDate DATE_2021_02_03 = LocalDate.parse("2021-02-03");
  private static final LocalDate DATE_2021_02_04 = LocalDate.parse("2021-02-04");
  private static final LocalDate DATE_2021_02_05 = LocalDate.parse("2021-02-05");
  private static final LocalDate DATE_2021_02_28 = LocalDate.parse("2021-02-28");
  private static final LocalDate DATE_2021_03_04 = LocalDate.parse("2021-03-04");

  private static final List<String> EMPTY_PROCESSING_GROUPS = Collections.emptyList();
  private static final List<String> STANDARD_BILLING = Collections.singletonList("STANDARD");
  private static final List<String> ADHOC_BILLING = Collections.singletonList("ADHOC");
  private static final List<String> ALL_BILLING = Arrays.asList("STANDARD", "ADHOC");

  private static final String PROCESSING_GROUP_X = "X";
  private static final String PROCESSING_GROUP_ALL = "ALL";
  private static final String PROCESSING_GROUP_DEFAULT = "DEFAULT";

  private static final String BILL_CYCLE_ID_WPDY = "WPDY";
  private static final String BILL_CYCLE_ID_WPMO = "WPMO";

  private static final Validation<DomainError, String> ACCOUNT_NOT_FOUND_1 = Validation
      .invalid(DomainError.of("ANF1", "Account not found 1"));
  private static final Validation<DomainError, BillingCycle> ACCOUNT_NOT_FOUND_2 = Validation
      .invalid(DomainError.of("ANF2", "Account not found 2"));
  public static final String ACCOUNT_TYPE_FUND = "FUND";
  public static final String ACCOUNT_TYPE_CHRG = "CHRG";
  private static final Seq<PendingMinimumCharge> EMPTY_PENDING_MINIMUM_CHARGES = io.vavr.collection.List.empty();
  private static final PendingMinimumCharge PENDING_MINIMUM_CHARGE =
      new PendingMinimumCharge("P00009", "P00001", "P00009",
          DATE_2021_02_04, DATE_2021_03_04, "MIN_CHG", BigDecimal.valueOf(10), DATE_2021_02_04, null);
  public static final BillTax BILL_TAX = new BillTax("billTaxId1", "billId", "", "", "", "", "N", new HashMap<>(), new BillTaxDetail[]{});
  private static final BillingAccount CHARGING_ACCOUNT = BillingAccount.builder()
      .billingCycle(new BillingCycle("WPDY", null, null))
      .accountId("1234")
      .currency("GBP")
      .accountType("CHRG")
      .businessUnit("BU123")
      .legalCounterparty("P00001")
      .childPartyId("P00009")
      .partyId("P00009")
      .subAccountType("CHRG")
      .subAccountId("567")
      .processingGroup(PROCESSING_GROUP_X)
      .build();

  private final MinimumChargeCalculationService minimumChargeCalculationService = buildDummyMinimumChargeCalculationService();

  private BillProcessingService billProcessingService;
  private PendingBill pendingBill;

  @Test
  void whenCompleteBillAndProcessingGroupSearchReturnsInvalidThenReturnInvalid() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, EMPTY_PROCESSING_GROUPS, ADHOC_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> ACCOUNT_NOT_FOUND_1),
        minimumChargeCalculationService,
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());
    pendingBill = pendingBill("Y", DATE_2021_02_05);

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult =
        billProcessingService.completeBill(pendingBill, EMPTY_PENDING_MINIMUM_CHARGES);

    assertThat(completeBillResult.isInvalid(), is(true));
    assertThat(completeBillResult.getError().get(0).getDomainError(), equalTo(ACCOUNT_NOT_FOUND_1.getError()));
  }

  @Test
  void whenCompleteBillAndAccountProcessingGroupIsNotInProcessingGroupListThenReturnDoNotCompleteDecision() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, EMPTY_PROCESSING_GROUPS, ADHOC_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> Validation.valid(PROCESSING_GROUP_X)),
        minimumChargeCalculationService,
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());
    pendingBill = pendingBill("Y", DATE_2021_02_05);

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult =
        billProcessingService.completeBill(pendingBill, EMPTY_PENDING_MINIMUM_CHARGES);

    assertThat(completeBillResult.isValid(), is(true));
    assertThat(completeBillResult.get().isShouldComplete(), is(false));
  }

  @Test
  @DisplayName("When complete bill and ALL is in processing group list and adhoc bill then return complete decision")
  void whenCompleteBillAndALLIsInProcessingGroupListAndAdhocBillThenReturnCompleteDecision() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList(PROCESSING_GROUP_ALL),
        ADHOC_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> Validation.valid(PROCESSING_GROUP_X)),
        minimumChargeCalculationService,
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());
    pendingBill = pendingBill("Y", DATE_2021_02_05);

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult =
        billProcessingService.completeBill(pendingBill, EMPTY_PENDING_MINIMUM_CHARGES);

    assertThat(completeBillResult.isValid(), is(true));
    assertThat(completeBillResult.get().isShouldComplete(), is(true));
    assertThat(completeBillResult.get().getCompleteBill().getProcessingGroup(), equalTo(PROCESSING_GROUP_X));
  }

  @Test
  @DisplayName("When complete bill and account processing group is NULL and DEFAULT is in processing group list and adhoc bill then return complete decision")
  void whenCompleteBillAndAccountProcessingGroupIsNULLAndDEFAULTIsInProcessingGroupListAndAdhocBillThenReturnCompleteDecision() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList(PROCESSING_GROUP_DEFAULT),
        ADHOC_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> Validation.valid(null)),
        minimumChargeCalculationService,
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());
    pendingBill = pendingBill("Y", DATE_2021_02_05);

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult =
        billProcessingService.completeBill(pendingBill, EMPTY_PENDING_MINIMUM_CHARGES);

    assertThat(completeBillResult.isValid(), is(true));
    assertThat(completeBillResult.get().isShouldComplete(), is(true));
    assertThat(completeBillResult.get().getCompleteBill().getProcessingGroup(), equalTo(null));
  }

  @Test
  void whenCompleteBillAndAccountProcessingGroupIsInProcessingGroupListAndAdhocBillThenReturnCompleteDecision() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList(PROCESSING_GROUP_X),
        ADHOC_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> Validation.valid(PROCESSING_GROUP_X)),
        minimumChargeCalculationService,
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());
    pendingBill = pendingBill("Y", DATE_2021_02_05);

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult =
        billProcessingService.completeBill(pendingBill, EMPTY_PENDING_MINIMUM_CHARGES);

    assertThat(completeBillResult.isValid(), is(true));
    assertThat(completeBillResult.get().isShouldComplete(), is(true));
    assertThat(completeBillResult.get().getCompleteBill().getProcessingGroup(), equalTo(PROCESSING_GROUP_X));
  }

  @Test
  void whenCompleteBillAndAdhocBillAndBillCycleIsWPMOThenReturnCompleteDecisionWithScheduleStartAndEndEqualToLogicalDate() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList(PROCESSING_GROUP_X),
        ADHOC_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> Validation.valid(PROCESSING_GROUP_X)),
        minimumChargeCalculationService,
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());
    pendingBill = pendingBill("Y", BILL_CYCLE_ID_WPMO, ACCOUNT_TYPE_CHRG, DATE_2021_02_01, DATE_2021_02_28);

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult =
        billProcessingService.completeBill(pendingBill, EMPTY_PENDING_MINIMUM_CHARGES);

    assertThat(completeBillResult.isValid(), is(true));
    assertThat(completeBillResult.get().isShouldComplete(), is(true));

    CompleteBill completeBill = completeBillResult.get().getCompleteBill();
    assertThat(completeBill.getBillCycleId(), is(BILL_CYCLE_ID_WPDY));
    assertThat(completeBill.getScheduleStart(), equalTo(DATE_2021_02_04));
    assertThat(completeBill.getScheduleEnd(), equalTo(DATE_2021_02_04));
  }

  @Test
  void whenCompleteBillAndNotAdhocBillAndEndDateAfterLogicalDateThenReturnDoNotCompleteDecision() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList(PROCESSING_GROUP_X),
        STANDARD_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> Validation.valid(PROCESSING_GROUP_X)),
        minimumChargeCalculationService,
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());
    pendingBill = pendingBill("N", DATE_2021_02_05);

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult =
        billProcessingService.completeBill(pendingBill, EMPTY_PENDING_MINIMUM_CHARGES);

    assertThat(completeBillResult.isValid(), is(true));
    assertThat(completeBillResult.get().isShouldComplete(), is(false));
    assertThat(completeBillResult.get().getCompleteBill(), is(nullValue()));
  }

  @Test
  void whenCompleteBillAndNotAdhocBillAndEndDateEqualToLogicalDateThenReturnCompleteDecision() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList(PROCESSING_GROUP_X),
        STANDARD_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> Validation.valid(PROCESSING_GROUP_X)),
        minimumChargeCalculationService,
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());
    pendingBill = pendingBill("N", DATE_2021_02_04);

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult =
        billProcessingService.completeBill(pendingBill, EMPTY_PENDING_MINIMUM_CHARGES);

    assertThat(completeBillResult.isValid(), is(true));
    assertThat(completeBillResult.get().isShouldComplete(), is(true));
    assertThat(completeBillResult.get().getCompleteBill().getProcessingGroup(), equalTo(PROCESSING_GROUP_X));
  }

  @Test
  void whenCompleteBillAndNotAdhocBillAndEndDateBeforeLogicalDateThenReturnCompleteDecision() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList(PROCESSING_GROUP_X),
        STANDARD_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> Validation.valid(PROCESSING_GROUP_X)),
        minimumChargeCalculationService,
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());
    pendingBill = pendingBill("N", DATE_2021_02_03);

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult =
        billProcessingService.completeBill(pendingBill, EMPTY_PENDING_MINIMUM_CHARGES);

    assertThat(completeBillResult.isValid(), is(true));
    assertThat(completeBillResult.get().isShouldComplete(), is(true));
    assertThat(completeBillResult.get().getCompleteBill().getProcessingGroup(), equalTo(PROCESSING_GROUP_X));
  }

  @Test
  void whenUpdateBillAndBillCycleIdSearchReturnsInvalidThenReturnInvalid() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList(PROCESSING_GROUP_X),
        STANDARD_BILLING,
        buildAccountDeterminationServiceForBillCycleSearch(
            accountId -> ACCOUNT_NOT_FOUND_1,
            (accountId, date) -> Validation.valid(billingCycle(BILL_CYCLE_ID_WPDY, DATE_2021_02_04, DATE_2021_02_04))
        ),
        minimumChargeCalculationService,
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());
    pendingBill = pendingBill(BILL_CYCLE_ID_WPDY, DATE_2021_02_04, DATE_2021_02_04);

    Validation<Seq<BillLineDomainError>, UpdateBillDecision> updateBillResult = billProcessingService.updateBill(pendingBill);

    assertThat(updateBillResult.isInvalid(), is(true));
    assertThat(updateBillResult.getError().get(0).getDomainError(), equalTo(ACCOUNT_NOT_FOUND_1.getError()));
  }

  @Test
  void whenUpdateBillAndBillCycleIdHasNotChangedThenReturnNotUpdateDecision() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList(PROCESSING_GROUP_X),
        STANDARD_BILLING,
        buildAccountDeterminationServiceForBillCycleSearch(
            accountId -> Validation.valid(BILL_CYCLE_ID_WPDY),
            (accountId, date) -> Validation.valid(billingCycle(BILL_CYCLE_ID_WPDY, DATE_2021_02_04, DATE_2021_02_04))
        ),
        minimumChargeCalculationService,
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());
    pendingBill = pendingBill(BILL_CYCLE_ID_WPDY, DATE_2021_02_04, DATE_2021_02_04);

    Validation<Seq<BillLineDomainError>, UpdateBillDecision> updateBillResult = billProcessingService.updateBill(pendingBill);

    assertThat(updateBillResult.isValid(), is(true));
    assertThat(updateBillResult.get().isShouldUpdate(), is(false));
    assertThat(updateBillResult.get().getBillCycleId(), is(nullValue()));
    assertThat(updateBillResult.get().getScheduleEnd(), is(nullValue()));
  }

  @Test
  void whenUpdateBillAndBillCycleSearchReturnsInvalidThenReturnInvalid() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList(PROCESSING_GROUP_X),
        STANDARD_BILLING,
        buildAccountDeterminationServiceForBillCycleSearch(
            accountId -> Validation.valid(BILL_CYCLE_ID_WPDY),
            (accountId, date) -> ACCOUNT_NOT_FOUND_2
        ),
        minimumChargeCalculationService,
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());
    pendingBill = pendingBill(BILL_CYCLE_ID_WPMO, DATE_2021_02_04, DATE_2021_03_04);

    Validation<Seq<BillLineDomainError>, UpdateBillDecision> updateBillResult = billProcessingService.updateBill(pendingBill);

    assertThat(updateBillResult.isInvalid(), is(true));
    assertThat(updateBillResult.getError().get(0).getDomainError(), equalTo(ACCOUNT_NOT_FOUND_2.getError()));
  }

  @Test
  void whenUpdateBillAndBillCycleIdHasChangedThenReturnUpdateDecision() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList(PROCESSING_GROUP_X),
        STANDARD_BILLING,
        buildAccountDeterminationServiceForBillCycleSearch(
            accountId -> Validation.valid(BILL_CYCLE_ID_WPDY),
            (accountId, date) -> {
              if (ACCOUNT_ID.equals(accountId) && DATE_2021_02_04.equals(date)) {
                return Validation.valid(billingCycle(BILL_CYCLE_ID_WPDY, DATE_2021_02_04, DATE_2021_02_04));
              }
              return ACCOUNT_NOT_FOUND_2;
            }
        ),
        minimumChargeCalculationService,
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());
    pendingBill = pendingBill(BILL_CYCLE_ID_WPMO, DATE_2021_02_04, DATE_2021_03_04);

    Validation<Seq<BillLineDomainError>, UpdateBillDecision> updateBillResult = billProcessingService.updateBill(pendingBill);

    assertThat(updateBillResult.isValid(), is(true));
    assertThat(updateBillResult.get().isShouldUpdate(), is(true));
    assertThat(updateBillResult.get().getBillCycleId(), equalTo(BILL_CYCLE_ID_WPDY));
    assertThat(updateBillResult.get().getScheduleEnd(), equalTo(DATE_2021_02_04));
  }

  @Test
  void whenUpdateBillAndBillCycleIdHasChangedAndLogicalDateIsGreaterThanStartDateCycleThenReturnUpdateDecisionAndEndDateIsLogicalDate() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_05, Collections.singletonList(PROCESSING_GROUP_X),
        STANDARD_BILLING,
        buildAccountDeterminationServiceForBillCycleSearch(
            accountId -> Validation.valid(BILL_CYCLE_ID_WPDY),
            (accountId, date) -> {
              if (ACCOUNT_ID.equals(accountId) && DATE_2021_02_04.equals(date)) {
                return Validation.valid(billingCycle(BILL_CYCLE_ID_WPDY, DATE_2021_02_04, DATE_2021_02_04));
              }
              return ACCOUNT_NOT_FOUND_2;
            }
        ),
        minimumChargeCalculationService,
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());
    pendingBill = pendingBill(BILL_CYCLE_ID_WPMO, DATE_2021_02_04, DATE_2021_03_04);

    Validation<Seq<BillLineDomainError>, UpdateBillDecision> updateBillResult = billProcessingService.updateBill(pendingBill);

    assertThat(updateBillResult.isValid(), is(true));
    assertThat(updateBillResult.get().isShouldUpdate(), is(true));
    assertThat(updateBillResult.get().getBillCycleId(), equalTo(BILL_CYCLE_ID_WPDY));
    assertThat(updateBillResult.get().getScheduleEnd(), equalTo(DATE_2021_02_05));
  }

  @Test
  @DisplayName("When Complete Bill with Processing Group ALL, two pending bills with Processing Group X and Adhoc flag Y and N then both return complete decision")
  void whenTwoPendingBillsWithDifferentAdhocFlagAndProcessingGroupGoInCompleteBillWithProcessingGroupAllThenBothComplete() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList(PROCESSING_GROUP_ALL),
        ALL_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> Validation.valid(PROCESSING_GROUP_X)),
        minimumChargeCalculationService,
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());
    TestPendingBill pendingBill1 = pendingBill("Y", DATE_2021_02_05);
    TestPendingBill pendingBill2 = pendingBill("N", DATE_2021_02_04);

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult1 =
        billProcessingService.completeBill(pendingBill1, EMPTY_PENDING_MINIMUM_CHARGES);
    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult2 =
        billProcessingService.completeBill(pendingBill2, EMPTY_PENDING_MINIMUM_CHARGES);

    assertThat(completeBillResult1.isValid(), is(true));
    assertThat(completeBillResult1.get().isShouldComplete(), is(true));
    assertThat(completeBillResult1.get().getCompleteBill().getProcessingGroup(), equalTo(PROCESSING_GROUP_X));
    assertThat(completeBillResult2.isValid(), is(true));
    assertThat(completeBillResult2.get().isShouldComplete(), is(true));
    assertThat(completeBillResult2.get().getCompleteBill().getProcessingGroup(), equalTo(PROCESSING_GROUP_X));
  }

  @Test
  @DisplayName("When bill should complete and has minimum charge then return correct result")
  void whenBillShouldBeCompletedAndTWeHaveMinimumChargeThenReturnCCorrectResult() {

    CompleteBillLine completeBillLine = buildMinimumChargeLine();

    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList(PROCESSING_GROUP_ALL),
        ALL_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> Validation.valid(PROCESSING_GROUP_X)),
        buildMinimumChargeCalculationService(true),
        buildDummyTaxCalculationService(),
        buildDummyRoundingService()
    );
    TestPendingBill pendingBill = pendingBill(null, ACCOUNT_TYPE_CHRG, null, DATE_2021_02_04);

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult =
        billProcessingService.completeBill(pendingBill, EMPTY_PENDING_MINIMUM_CHARGES);

    assertThat(completeBillResult.isValid(), is(true));
    assertThat(completeBillResult.get().isShouldComplete(), is(true));
    assertThat(completeBillResult.get().getCompleteBill().getBillAmount(), is(BigDecimal.valueOf(8.0)));
    assertThat(io.vavr.collection.List.of(completeBillResult.get().getPendingMinimumCharges()), hasItem(PENDING_MINIMUM_CHARGE));

    assertThat(io.vavr.collection.List.of(completeBillResult.get().getCompleteBill().getBillLines()), iterableWithSize(2));
    assertThat(io.vavr.collection.List.of(completeBillResult.get().getCompleteBill().getBillLines()), hasItem(completeBillLine));
  }

  @Test
  void whenBillShouldCompleteThenResultShouldHaveRoundedAmountsAccordingToTheRoundingService() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList(PROCESSING_GROUP_X),
        STANDARD_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> Validation.valid(PROCESSING_GROUP_X)),
        minimumChargeCalculationService,
        buildDummyTaxCalculationService(),
        (amount, currency) -> amount.setScale(2, RoundingMode.HALF_UP));
    TestPendingBillLineCalculation pendingBillLineCalculation1 = new TestPendingBillLineCalculation("bill_ln_calc_id_1", "class1", "type1",
        BigDecimal.valueOf(10.0151), "Y", "rateType", BigDecimal.ONE);
    TestPendingBillLineServiceQuantity pendingBillLineServiceQuantities = new TestPendingBillLineServiceQuantity("TOTAL_AMT",
        BigDecimal.valueOf(11.0149));
    TestPendingBillLine pendingBillLine = TestPendingBillLine.builder()
        .billLineId("abc123")
        .pendingBillLineCalculations(new TestPendingBillLineCalculation[]{pendingBillLineCalculation1})
        .pendingBillLineServiceQuantities(new TestPendingBillLineServiceQuantity[]{pendingBillLineServiceQuantities})
        .build();
    pendingBill = pendingBill("N", DATE_2021_02_03).withPendingBillLines(new TestPendingBillLine[]{pendingBillLine});

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult =
        billProcessingService.completeBill(pendingBill, EMPTY_PENDING_MINIMUM_CHARGES);

    assertThat(completeBillResult.isValid(), is(true));
    assertThat(completeBillResult.get().isShouldComplete(), is(true));
    CompleteBill completeBill = completeBillResult.get().getCompleteBill();
    assertThat(completeBill.getProcessingGroup(), equalTo(PROCESSING_GROUP_X));
    assertThat(completeBill.getBillAmount().toPlainString(), is("10.02"));
    CompleteBillLine completeBillLine = completeBill.getBillLines()[0];
    assertThat(completeBillLine.getTotalAmount().toPlainString(), is("10.02"));
    CompleteBillLineCalculation completeBillLineCalc = completeBillLine.getBillLineCalculations()[0];
    assertThat(completeBillLineCalc.getAmount().toPlainString(), is("10.02"));
    CompleteBillLineServiceQuantity completeBillLineServiceQuantity = completeBillLine.getBillLineServiceQuantities()[0];
    assertThat(completeBillLineServiceQuantity.getServiceQuantityValue().toPlainString(), is("11.01"));
  }

  @Test
  void whenTheBillIsEmptyAndMinChargeShouldNotCompleteThenReturnDoNotComplete() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_03, Collections.singletonList(PROCESSING_GROUP_X),
        STANDARD_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> Validation.valid(PROCESSING_GROUP_X)),
        buildMinimumChargeCalculationService(false),
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult =
        billProcessingService.completeBill(null, io.vavr.collection.List.of(PENDING_MINIMUM_CHARGE));

    assertThat(completeBillResult.isValid(), is(true));
    assertThat(completeBillResult.get().isShouldComplete(), is(false));
  }

  @Test
  void whenTheBillIsEmptyAndBillingTypeIsAdhocThenReturnDoNotComplete() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList(PROCESSING_GROUP_X),
        ADHOC_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> Validation.valid(PROCESSING_GROUP_X)),
        buildMinimumChargeCalculationService(false),
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult =
        billProcessingService.completeBill(null, io.vavr.collection.List.of(PENDING_MINIMUM_CHARGE));

    assertThat(completeBillResult.isValid(), is(true));
    assertThat(completeBillResult.get().isShouldComplete(), is(false));
  }

  @Test
  void whenTheBillIsEmptyAndMinChargeShouldNotCompleteForProcessingGrupThenReturnDoNotComplete() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList(PROCESSING_GROUP_DEFAULT),
        STANDARD_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> Validation.valid(PROCESSING_GROUP_X)),
        buildMinimumChargeCalculationService(false),
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult =
        billProcessingService.completeBill(null, io.vavr.collection.List.of(PENDING_MINIMUM_CHARGE));

    assertThat(completeBillResult.isValid(), is(true));
    assertThat(completeBillResult.get().isShouldComplete(), is(false));
  }

  @Test
  void whenTheBillIsEmptyAndMinChargeShouldCompleteThenReturnCompleteBill() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList(PROCESSING_GROUP_X),
        STANDARD_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> Validation.valid(PROCESSING_GROUP_X)),
        buildMinimumChargeCalculationService(true),
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult =
        billProcessingService.completeBill(null, io.vavr.collection.List.of(PENDING_MINIMUM_CHARGE));

    assertThat(completeBillResult.isValid(), is(true));
    assertThat(completeBillResult.get().isShouldComplete(), is(true));

    CompleteBill completeBill = completeBillResult.get().getCompleteBill();
    assertThat(completeBill, equalTo(buildCompleteBill()));
  }

  @Test
  void whenPendingBillHasMiscalculationFlagYesThenReturnDoNotComplete() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, EMPTY_PROCESSING_GROUPS, ADHOC_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> ACCOUNT_NOT_FOUND_1),
        minimumChargeCalculationService,
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());
    pendingBill = pendingBill("Y", DATE_2021_02_05).withMiscalculationFlag("Y");

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult =
        billProcessingService.completeBill(pendingBill, EMPTY_PENDING_MINIMUM_CHARGES);

    assertThat(completeBillResult.isInvalid(), is(true));
    assertThat(completeBillResult.getError().get(0).getDomainError(), is(DomainError
        .of("LINE_CALC_MISCALCULATION_DETECTED", "Bill Line Calc miscalculation detected on bill. Check Splunk for billable item ids.")));
  }

  @Test
  void whenTheBillIsEmptyAndMinChargeShouldNotCompleteThenReturnPendingMinChage() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList(PROCESSING_GROUP_X),
        STANDARD_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> Validation.valid(PROCESSING_GROUP_X)),
        buildMinimumChargeCalculationService(false),
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult =
        billProcessingService.completeBill(null, io.vavr.collection.List.of(PENDING_MINIMUM_CHARGE));

    assertThat(completeBillResult.isValid(), is(true));
    assertThat(completeBillResult.get().isShouldComplete(), is(false));

    PendingMinimumCharge[] pendingMinimumCharges = completeBillResult.get().getPendingMinimumCharges();
    assertThat(io.vavr.collection.List.of(pendingMinimumCharges), hasItem(PENDING_MINIMUM_CHARGE));
  }

  @Test
  void whenTheBillIsEmptyAndMinChargeShouldNotCompleteForProcessingGrupThenReturnPendingMinChage() {
    billProcessingService = new DefaultBillProcessingService(DATE_2021_02_04, Collections.singletonList("ABC"),
        STANDARD_BILLING,
        buildAccountDeterminationServiceForProcessingGroupSearch(accountId -> Validation.valid(PROCESSING_GROUP_X)),
        buildMinimumChargeCalculationService(true),
        buildDummyTaxCalculationService(),
        buildDummyRoundingService());

    Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillResult =
        billProcessingService.completeBill(null, io.vavr.collection.List.of(PENDING_MINIMUM_CHARGE));

    assertThat(completeBillResult.isValid(), is(true));
    assertThat(completeBillResult.get().isShouldComplete(), is(false));

    PendingMinimumCharge[] pendingMinimumCharges = completeBillResult.get().getPendingMinimumCharges();
    assertThat(io.vavr.collection.List.of(pendingMinimumCharges), hasItem(PENDING_MINIMUM_CHARGE));
  }

  ///

  private static TestPendingBill pendingBill(String adhocBillIndicator, LocalDate scheduleEnd) {
    return pendingBill(adhocBillIndicator, null, ACCOUNT_TYPE_FUND, null, scheduleEnd);
  }

  private static TestPendingBill pendingBill(String billCycleId, LocalDate scheduleStart, LocalDate scheduleEnd) {
    return pendingBill(null, billCycleId, ACCOUNT_TYPE_FUND, scheduleStart, scheduleEnd);
  }

  private static TestPendingBill pendingBill(String billCycleId, String accountType, LocalDate scheduleStart, LocalDate scheduleEnd) {
    return pendingBill(null, billCycleId, accountType, scheduleStart, scheduleEnd);
  }

  private static TestPendingBill pendingBill(String adhocBillIndicator, String billCycleId, String accounType,
      LocalDate scheduleStart, LocalDate scheduleEnd) {
    return TestPendingBill.builder()
        .accountId(ACCOUNT_ID)
        .accountType(accounType)
        .adhocBillIndicator(adhocBillIndicator)
        .billCycleId(billCycleId)
        .scheduleStart(scheduleStart == null ? null : Date.valueOf(scheduleStart))
        .scheduleEnd(scheduleEnd == null ? null : Date.valueOf(scheduleEnd))
        .pendingBillLines(getPendingBillLines())
        .miscalculationFlag("N")
        .build();
  }

  private static TestPendingBillLine[] getPendingBillLines() {
    TestPendingBillLineCalculation pendingBillLineCalculations = new TestPendingBillLineCalculation("bill_ln_calc_id_1", "class1", "type1",
        BigDecimal.valueOf(1.0), "Y", "rateType", BigDecimal.ONE);
    TestPendingBillLineServiceQuantity pendingBillLineServiceQuantities = new TestPendingBillLineServiceQuantity("F_M_AMT", BigDecimal.ONE);
    TestPendingBillLine pendingBillLine = TestPendingBillLine.builder()
        .billLineId("abc123")
        .pendingBillLineCalculations(new TestPendingBillLineCalculation[]{pendingBillLineCalculations})
        .pendingBillLineServiceQuantities(new TestPendingBillLineServiceQuantity[]{pendingBillLineServiceQuantities})
        .build();

    return new TestPendingBillLine[]{pendingBillLine};
  }

  private static BillingCycle billingCycle(String code, LocalDate startDate, LocalDate endDate) {
    return new BillingCycle(code, startDate, endDate);
  }

  private static AccountDeterminationService buildAccountDeterminationServiceForBillCycleSearch(
      Function<String, Validation<DomainError, String>> getBillingCycleCodeForAccountId,
      Function2<String, LocalDate, Validation<DomainError, BillingCycle>> getBillingCycleForAccountId) {
    return buildAccountDeterminationService(getBillingCycleForAccountId, THROW_EXCEPTION_1, getBillingCycleCodeForAccountId);
  }

  private static AccountDeterminationService buildAccountDeterminationServiceForProcessingGroupSearch(
      Function<String, Validation<DomainError, String>> getProcessingGroupForAccountId) {
    return buildAccountDeterminationService(THROW_EXCEPTION_2, getProcessingGroupForAccountId, THROW_EXCEPTION_1);
  }

  private static AccountDeterminationService buildAccountDeterminationService(
      Function2<String, LocalDate, Validation<DomainError, BillingCycle>> getBillingCycleForAccountId,
      Function<String, Validation<DomainError, String>> getProcessingGroupForAccountId,
      Function<String, Validation<DomainError, String>> getBillingCycleCodeForAccountId) {

    return new AccountDeterminationService() {
      @Override
      public Validation<DomainError, BillingAccount> findAccount(AccountKey accountKey) {
        throw new UnsupportedOperationException("Not implemented");
      }

      @Override
      public Validation<DomainError, BillingCycle> getBillingCycleForAccountId(String accountId, LocalDate date) {
        return getBillingCycleForAccountId.apply(accountId, date);
      }

      @Override
      public Validation<DomainError, String> getProcessingGroupForAccountId(String accountId) {
        return getProcessingGroupForAccountId.apply(accountId);
      }

      @Override
      public Validation<DomainError, String> getBillingCycleCodeForAccountId(String accountId) {
        return getBillingCycleCodeForAccountId.apply(accountId);
      }

      @Override
      public Validation<DomainError, BillingAccount> getChargingAccountByPartyId(String partyId, String legalCounterParty,
          String currency) {
        return Validation.valid(CHARGING_ACCOUNT);
      }
    };
  }

  private static MinimumChargeCalculationService buildDummyMinimumChargeCalculationService() {
    return new MinimumChargeCalculationService() {
      @Override
      public MinimumChargeResult compute(CompleteBill bill, Seq<PendingMinimumCharge> pendingMinimumCharges) {
        return new MinimumChargeResult(bill, io.vavr.collection.List.empty());
      }

      @Override
      public Boolean shouldCompleteAnyPending(Seq<PendingMinimumCharge> pendingMinimumCharges) {
        return null;
      }


      @Override
      public MinimumChargeResult compute(BillingAccount account, Seq<PendingMinimumCharge> pendingMinimumCharges) {
        return new MinimumChargeResult(null, io.vavr.collection.List.empty());
      }

      @Override
      public Seq<PendingMinimumCharge> getPendingMinCharges(Seq<PendingMinimumCharge> pending) {
        return null;
      }
    };
  }

  private static MinimumChargeCalculationService buildMinimumChargeCalculationService(boolean shouldComplete) {
    return new MinimumChargeCalculationService() {
      @Override
      public MinimumChargeResult compute(CompleteBill bill, Seq<PendingMinimumCharge> pendingMinimumCharges) {
        CompleteBillLine completeBillLine = buildMinimumChargeLine();
        return new MinimumChargeResult(bill.addBillLines(io.vavr.collection.List.of(completeBillLine)),
            io.vavr.collection.List.of(PENDING_MINIMUM_CHARGE));
      }

      @Override
      public Boolean shouldCompleteAnyPending(Seq<PendingMinimumCharge> pendingMinimumCharges) {
        return shouldComplete;
      }

      @Override
      public MinimumChargeResult compute(BillingAccount account, Seq<PendingMinimumCharge> pendingMinimumCharges) {
        return new MinimumChargeResult(buildCompleteBill(), io.vavr.collection.List.empty());
      }

      @Override
      public Seq<PendingMinimumCharge> getPendingMinCharges(Seq<PendingMinimumCharge> pending) {
        return io.vavr.collection.List.of(PENDING_MINIMUM_CHARGE);
      }
    };
  }

  private static TaxCalculationService buildDummyTaxCalculationService() {
    return pendingBill -> Validation.valid(BILL_TAX);
  }

  private static RoundingService buildDummyRoundingService() {
    return (BigDecimal amount, String currency) -> amount;
  }

  private static CompleteBill buildCompleteBill() {
    return CompleteBill.builder()
        .partyId("P00009")
        .legalCounterpartyId("P00001")
        .accountId("1234")
        .billSubAccountId("567")
        .accountType("CHRG")
        .businessUnit("BU123")
        .billCycleId("WPDY")
        .scheduleStart(DATE_2021_02_04)
        .scheduleEnd(DATE_2021_02_04)
        .currency("GBP")
        .processingGroup(PROCESSING_GROUP_X)
        .billAmount(BigDecimal.valueOf(7))
        .billLines(io.vavr.collection.List.of(buildMinimumChargeLine()).toJavaArray(CompleteBillLine[]::new))
        .billTax(BILL_TAX)
        .build();
  }

  private static CompleteBillLine buildMinimumChargeLine() {
    return CompleteBillLine.builder().billLinePartyId("P00008")
        .productClass("MISCELLANEOUS")
        .productIdentifier("MINCHRGP")
        .pricingCurrency("GBP")
        .quantity(BigDecimal.ONE)
        .priceLineId("price_assign4")
        .totalAmount(BigDecimal.valueOf(7))
        .billLineCalculations(new CompleteBillLineCalculation[]{
            CompleteBillLineCalculation.builder()
                .calculationLineClassification("BASE_CHG")
                .calculationLineType("TOTAL_BB")
                .amount(BigDecimal.valueOf(7))
                .includeOnBill("Y")
                .rateType("MIN_CHG")
                .rateValue(BigDecimal.valueOf(25))
                .build()
        })
        .build();
  }
}