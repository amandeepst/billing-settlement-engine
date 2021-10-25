package com.worldpay.pms.bse.domain.model.completebill;

import static com.worldpay.pms.bse.domain.common.Utils.BILL_CYCLE_WPDY;

import com.worldpay.pms.bse.domain.account.BillingAccount;
import com.worldpay.pms.bse.domain.account.BillingCycle;
import com.worldpay.pms.bse.domain.common.Utils;
import com.worldpay.pms.bse.domain.model.PendingBill;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Objects;
import java.util.function.BiFunction;
import lombok.Builder;
import lombok.Value;
import lombok.With;

@With
@Builder
@Value
public class CompleteBill {

  String billId;
  String partyId;
  String legalCounterpartyId;
  String accountId;
  String billSubAccountId;
  String accountType;
  String businessUnit;
  String billCycleId;
  LocalDate scheduleStart;
  LocalDate scheduleEnd;
  String currency;
  String billReference;
  String adhocBillIndicator;
  String settlementSubLevelType;
  String settlementSubLevelValue;
  String granularity;
  String granularityKeyValue;
  LocalDate debtDate;
  String debtMigrationType;
  String overpaymentIndicator;
  String releaseWAFIndicator;
  String releaseReserveIndicator;
  String fastestPaymentRouteIndicator;
  String caseIdentifier;
  String individualBillIndicator;
  String manualBillNarrative;

  String processingGroup;
  BigDecimal billAmount;

  CompleteBillLine[] billLines;
  BillTax billTax;

  public static CompleteBill from(PendingBill pendingBill, LocalDate logicalDate, BiFunction<BigDecimal, String, BigDecimal> rounder) {
    return from(pendingBill, logicalDate, null, null, rounder);
  }

  public static CompleteBill from(PendingBill pendingBill, LocalDate logicalDate, String processingGroup,
      BiFunction<BigDecimal, String, BigDecimal> rounder) {
    return from(pendingBill, logicalDate, processingGroup, null, rounder);
  }

  public static CompleteBill from(PendingBill pendingBill, LocalDate logicalDate, String processingGroup, BillTax billTax,
      BiFunction<BigDecimal, String, BigDecimal> rounder) {
    String currency = pendingBill.getCurrency();
    Seq<CompleteBillLine> completeBillLines = List.of(pendingBill.getPendingBillLines())
        .map(line -> CompleteBillLine.from(line, amount -> rounder.apply(amount, currency)));
    BigDecimal totalAmount = completeBillLines.map(CompleteBillLine::getTotalAmount).reduce(BigDecimal::add);
    BigDecimal computedTotalAmount = rounder.apply(totalAmount, currency);

    String billCycleId = pendingBill.isAdhocBill() ? BILL_CYCLE_WPDY : pendingBill.getBillCycleId();
    LocalDate scheduleStart = pendingBill.isAdhocBill() ? logicalDate : Utils.getLocalDate(pendingBill.getScheduleStart());
    LocalDate scheduleEnd = pendingBill.isAdhocBill() ? logicalDate : Utils.getLocalDate(pendingBill.getScheduleEnd());

    return new CompleteBill(
        pendingBill.getBillId(),
        pendingBill.getPartyId(),
        pendingBill.getLegalCounterpartyId(),
        pendingBill.getAccountId(),
        pendingBill.getBillSubAccountId(),
        pendingBill.getAccountType(),
        pendingBill.getBusinessUnit(),
        billCycleId,
        scheduleStart,
        scheduleEnd,
        currency,
        pendingBill.getBillReference(),
        pendingBill.getAdhocBillIndicator(),
        pendingBill.getSettlementSubLevelType(),
        pendingBill.getSettlementSubLevelValue(),
        pendingBill.getGranularity(),
        pendingBill.getGranularityKeyValue(),
        Utils.getLocalDate(pendingBill.getDebtDate()),
        pendingBill.getDebtMigrationType(),
        pendingBill.getOverpaymentIndicator(),
        pendingBill.getReleaseWAFIndicator(),
        pendingBill.getReleaseReserveIndicator(),
        pendingBill.getFastestPaymentRouteIndicator(),
        pendingBill.getCaseIdentifier(),
        pendingBill.getIndividualBillIndicator(),
        pendingBill.getManualBillNarrative(),
        processingGroup,
        computedTotalAmount,
        completeBillLines.toJavaArray(CompleteBillLine[]::new),
        billTax
    );
  }

  public static CompleteBill from(BillingCycle minChargeCycle, BillingAccount billingAccount) {
    String billReference = billingAccount.getAccountId() + Objects.toString(minChargeCycle.getStartDate(), "") +
        Objects.toString(null, "") + "N" + "N" + "N" + "N";

    return new CompleteBill(
        null,
        billingAccount.getPartyId(),
        billingAccount.getLegalCounterparty(),
        billingAccount.getAccountId(),
        billingAccount.getSubAccountId(),
        billingAccount.getAccountType(),
        billingAccount.getBusinessUnit(),
        minChargeCycle.getCode(),
        minChargeCycle.getStartDate(),
        minChargeCycle.getEndDate(),
        billingAccount.getCurrency(),
        billReference,
        "N",
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        "N",
        "N",
        "N",
        null,
        "N",
        "N",
        billingAccount.getProcessingGroup(),
        BigDecimal.ZERO,
        new CompleteBillLine[0],
        null
    );

  }

  public CompleteBill addBillLines(Seq<CompleteBillLine> billLines) {
    if (billLines.isEmpty()) {
      return this;
    }
    return this.withBillLines(List.of(getBillLines()).appendAll(billLines).toJavaArray(CompleteBillLine[]::new))
        .withBillAmount(getBillAmount().add(billLines.map(CompleteBillLine::getTotalAmount).reduce(BigDecimal::add)));
  }
}
