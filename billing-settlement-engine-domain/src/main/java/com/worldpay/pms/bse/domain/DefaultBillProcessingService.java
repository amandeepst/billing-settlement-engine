package com.worldpay.pms.bse.domain;

import static com.worldpay.pms.bse.domain.common.Utils.max;

import com.worldpay.pms.bse.domain.account.AccountDeterminationService;
import com.worldpay.pms.bse.domain.account.BillingAccount;
import com.worldpay.pms.bse.domain.account.BillingCycle;
import com.worldpay.pms.bse.domain.common.BillLineDomainError;
import com.worldpay.pms.bse.domain.common.ErrorCatalog;
import com.worldpay.pms.bse.domain.mincharge.MinimumChargeCalculationService;
import com.worldpay.pms.bse.domain.model.PendingBill;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBill;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillDecision;
import com.worldpay.pms.bse.domain.model.completebill.UpdateBillDecision;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumChargeResult;
import com.worldpay.pms.bse.domain.model.mincharge.PendingMinimumCharge;
import com.worldpay.pms.bse.domain.tax.TaxCalculationService;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;
import java.time.LocalDate;
import java.util.List;
import java.util.function.IntFunction;

public class DefaultBillProcessingService implements BillProcessingService {

  private static final String ALL_GROUPS = "ALL";
  private static final String DEFAULT_GROUP = "DEFAULT";
  private static final String ADHOC_BILLING = "ADHOC";
  private static final String STANDARD_BILLING = "STANDARD";
  private static final String CHARGE_ACCOUNT_TYPE = "CHRG";

  private final LocalDate logicalDate;
  private final List<String> processingGroups;
  private final List<String> billingTypes;
  private final AccountDeterminationService accountDeterminationService;
  private final MinimumChargeCalculationService minimumChargeCalculationService;
  private final TaxCalculationService taxCalculationService;
  private final RoundingService roundingService;

  public DefaultBillProcessingService(LocalDate logicalDate, List<String> processingGroups, List<String> billingTypes,
      AccountDeterminationService accountDeterminationService,
      MinimumChargeCalculationService minimumChargeCalculationService,
      TaxCalculationService taxCalculationService,
      RoundingService roundingService) {
    this.logicalDate = logicalDate;
    this.processingGroups = processingGroups;
    this.billingTypes = billingTypes;
    this.accountDeterminationService = accountDeterminationService;
    this.minimumChargeCalculationService = minimumChargeCalculationService;
    this.taxCalculationService = taxCalculationService;
    this.roundingService = roundingService;
  }

  @Override
  public Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBill(PendingBill pendingBill,
      Seq<PendingMinimumCharge> pendingMinimumCharges) {

    if (pendingBill == null) {
      return processPendingMinCharge(pendingMinimumCharges);
    } else if ("Y".equals(pendingBill.getMiscalculationFlag())) {
      return invalidCompleteDecision(ErrorCatalog.lineCalcMiscalculationDetected());
    } else {
      Validation<DomainError, String> processingGroup =
          accountDeterminationService.getProcessingGroupForAccountId(pendingBill.getAccountId());
      if (processingGroup.isInvalid()) {
        return invalidCompleteDecision(processingGroup.getError());
      }
      if (!shouldCompleteForProcessingGroup(processingGroup.get())) {
        return doNotCompleteDecision();
      }

      if (shouldCompleteForBillingType(pendingBill)) {
        CompleteBill completeBill = CompleteBill.from(pendingBill, logicalDate, processingGroup.get(), roundingService::roundAmount);
        return completeBillWithMinChargeAndTax(completeBill, pendingMinimumCharges);
      }

      return doNotCompleteDecision();
    }
  }

  @Override
  public Validation<Seq<BillLineDomainError>, UpdateBillDecision> updateBill(PendingBill pendingBill) {
    Validation<DomainError, String> newBillCycleId =
        accountDeterminationService.getBillingCycleCodeForAccountId(pendingBill.getAccountId());
    if (newBillCycleId.isInvalid()) {
      return invalidUpdateDecision(newBillCycleId.getError());
    }

    if (newBillCycleId.get().equals(pendingBill.getBillCycleId())) {
      return doNotUpdateDecision();
    }

    Validation<DomainError, BillingCycle> newBillCycle = accountDeterminationService
        .getBillingCycleForAccountId(pendingBill.getAccountId(), pendingBill.getScheduleStart().toLocalDate());
    if (newBillCycle.isInvalid()) {
      return invalidUpdateDecision(newBillCycle.getError());
    }

    return updateDecision(newBillCycle.get(), logicalDate);
  }

  private Validation<Seq<BillLineDomainError>, CompleteBillDecision> processPendingMinCharge(
      Seq<PendingMinimumCharge> pendingMinimumCharges) {
    if (!minimumChargeCalculationService.shouldCompleteAnyPending(pendingMinimumCharges)) {
      return doNotCompleteDecision(minimumChargeCalculationService.getPendingMinCharges(pendingMinimumCharges)
          .toJavaArray(PendingMinimumCharge[]::new));
    }
    Validation<DomainError, BillingAccount> chargingAccount =
        accountDeterminationService.getChargingAccountByPartyId(
            pendingMinimumCharges.get(0).getBillPartyId(),
            pendingMinimumCharges.get(0).getLegalCounterpartyId(),
            pendingMinimumCharges.get(0).getCurrency());
    if (chargingAccount.isInvalid()) {
      return invalidCompleteDecision(chargingAccount.getError());
    }
    if (!shouldCompleteForProcessingGroup(chargingAccount.get().getProcessingGroup())) {
      return doNotCompleteDecision(minimumChargeCalculationService.getPendingMinCharges(pendingMinimumCharges)
          .toJavaArray(PendingMinimumCharge[]::new));
    }

    MinimumChargeResult minimumChargeResult = minimumChargeCalculationService.compute(chargingAccount.get(), pendingMinimumCharges);
    return completeBillWithTax(minimumChargeResult);
  }

  private boolean shouldCompleteForProcessingGroup(String processingGroup) {
    String processingGroupOrDefault = processingGroup != null ? processingGroup : DEFAULT_GROUP;
    return processingGroups.contains(ALL_GROUPS) || processingGroups.contains(processingGroupOrDefault);
  }

  /**
   * Would complete bill if Billing Type is `ADHOC` and current pending bill has adhoc_flag = `Y` or Billing Type is `STANDARD` and the
   * logical date is greater or equal to the bill cycle schedule end date
   *
   * @param pendingBill Pending Bill
   * @return true if bill should be completed
   */
  private boolean shouldCompleteForBillingType(PendingBill pendingBill) {

    return
        //
        (pendingBill.isAdhocBill() && billingTypes.contains(ADHOC_BILLING)) ||
            (logicalDateIsOnOrAfterBillScheduleEnd(logicalDate, pendingBill) && billingTypes.contains(STANDARD_BILLING));
  }

  private static boolean logicalDateIsOnOrAfterBillScheduleEnd(LocalDate logicalDate, PendingBill pendingBill) {
    LocalDate billScheduleEnd = pendingBill.getScheduleEnd().toLocalDate();
    return logicalDate.isEqual(billScheduleEnd) || logicalDate.isAfter(billScheduleEnd);
  }

  private static Validation<Seq<BillLineDomainError>, CompleteBillDecision> doNotCompleteDecision() {
    return Validation.valid(CompleteBillDecision.doNotCompleteDecision());
  }

  private static Validation<Seq<BillLineDomainError>, CompleteBillDecision> doNotCompleteDecision(
      PendingMinimumCharge[] pendingMinimumCharges) {
    return Validation.valid(CompleteBillDecision.doNotCompleteDecision(pendingMinimumCharges));
  }

  private Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillWithMinChargeAndTax(CompleteBill bill,
      Seq<PendingMinimumCharge> pendingMinimumCharges) {
    if (CHARGE_ACCOUNT_TYPE.equals(bill.getAccountType())) {
      MinimumChargeResult minimumChargeResult = minimumChargeCalculationService.compute(bill, pendingMinimumCharges);
      return completeBillWithTax(minimumChargeResult);
    }
    Validation<Seq<BillLineDomainError>, CompleteBill> valid = Validation.valid(bill);
    return valid.map(cb -> CompleteBillDecision.completeDecision(cb, null));
  }

  private Validation<Seq<BillLineDomainError>, CompleteBillDecision> completeBillWithTax(MinimumChargeResult minimumChargeResult) {
    CompleteBill completeBill = minimumChargeResult.getCompleteBill();
    return taxCalculationService.calculate(completeBill).map(completeBill::withBillTax)
        .map(cb -> CompleteBillDecision.completeDecision(cb,
            getArrayOrNull(minimumChargeResult.getPendingMinimumCharges(), PendingMinimumCharge[]::new)));
  }

  private static Validation<Seq<BillLineDomainError>, CompleteBillDecision> invalidCompleteDecision(DomainError domainError) {
    return Validation.invalid(BillLineDomainError.from(domainError).toSeq());
  }

  private static Validation<Seq<BillLineDomainError>, UpdateBillDecision> doNotUpdateDecision() {
    return Validation.valid(UpdateBillDecision.doNotUpdateDecision());
  }

  private static Validation<Seq<BillLineDomainError>, UpdateBillDecision> updateDecision(BillingCycle billCycle, LocalDate logicalDate) {
    return Validation.valid(UpdateBillDecision.updateDecision(billCycle.getCode(), max(billCycle.getEndDate(), logicalDate)));
  }

  private static Validation<Seq<BillLineDomainError>, UpdateBillDecision> invalidUpdateDecision(DomainError domainError) {
    return Validation.invalid(BillLineDomainError.from(domainError).toSeq());
  }

  private static <T> T[] getArrayOrNull(Seq<T> seq, IntFunction<T[]> arrayFactory) {
    return seq.isEmpty() ? null : seq.toJavaArray(arrayFactory);
  }
}
