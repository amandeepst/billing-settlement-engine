package com.worldpay.pms.bse.domain.mincharge;

import com.worldpay.pms.bse.domain.RoundingService;
import com.worldpay.pms.bse.domain.account.BillingAccount;
import com.worldpay.pms.bse.domain.account.BillingCycle;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBill;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLine;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLineCalculation;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumCharge;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumChargeKey;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumChargeResult;
import com.worldpay.pms.bse.domain.model.mincharge.PendingMinimumCharge;
import io.vavr.Tuple;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Seq;
import io.vavr.collection.Set;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import java.math.BigDecimal;
import java.time.LocalDate;

public class DefaultMinimumChargeCalculationService implements MinimumChargeCalculationService {

  private static final Seq<String> PRODUCT_CLASSES = List.of("PREMIUM", "ACQUIRED");

  private final LocalDate logicalDate;
  private final MinimumChargeStore minimumCharges;
  private final RoundingService roundingService;

  public DefaultMinimumChargeCalculationService(LocalDate logicalDate, MinimumChargeStore minimumCharges, RoundingService roundingService) {
    this.logicalDate = logicalDate;
    this.minimumCharges = minimumCharges;
    this.roundingService = roundingService;
  }

  @Override
  public MinimumChargeResult compute(BillingAccount account, Seq<PendingMinimumCharge> pendingMinimumCharges) {
    BillingCycle minChargeCycle = getCompletionMinimumCharge(pendingMinimumCharges).get().getCycle();
    CompleteBill completeBill = CompleteBill.from(minChargeCycle, account);
    return compute(completeBill, pendingMinimumCharges);
  }

  @Override
  public MinimumChargeResult compute(CompleteBill bill, Seq<PendingMinimumCharge> pendingMinimumCharges) {
    Map<MinimumCharge, PendingMinimumCharge> pending = computePending(Option.of(bill), pendingMinimumCharges);

    Seq<CompleteBillLine> completeBillLines = pending.filter((minimumCharge, pendingMinimumCharge) ->
        shouldComplete(pendingMinimumCharge) && isMinChargeApplicable(pendingMinimumCharge, minimumCharge))
        .map(t -> CompleteBillLine.from(t._1(), computeAmount(t._1().getRate(), t._2().getApplicableCharges()),
            amount -> roundingService.roundAmount(amount, bill.getCurrency())));

    return new MinimumChargeResult(bill.addBillLines(completeBillLines),
        getPendingMinCharges(pending.values()));
  }

  @Override
  public Boolean shouldCompleteAnyPending(Seq<PendingMinimumCharge> pendingMinimumCharges) {
    return !getCompletionMinimumCharge(pendingMinimumCharges).isEmpty();
  }

  @Override
  public Seq<PendingMinimumCharge> getPendingMinCharges(Seq<PendingMinimumCharge> pending) {
    return computePending(Option.none(), pending)
        .filter((minimumCharge, pendingMinimumCharge) -> !shouldComplete(pendingMinimumCharge)
            && pendingMinimumCharge.getApplicableCharges().compareTo(BigDecimal.ZERO) != 0)
        .values();

  }

  private Set<MinimumCharge> getCompletionMinimumCharge(Seq<PendingMinimumCharge> pendingMinimumCharges) {
    return computePending(Option.none(), pendingMinimumCharges)
        .filter((minimumCharge, pendingMinimumCharge) -> shouldComplete(pendingMinimumCharge))
        .keySet();
  }

  private Map<MinimumCharge, PendingMinimumCharge> computePending(Option<CompleteBill> bill,
      Seq<PendingMinimumCharge> pendingMinimumCharges) {
    Map<MinimumChargeKey, PendingMinimumCharge> pendingMinimumChargesAsMap = getPendingMinimumChargesAsMap(pendingMinimumCharges);
    Map<MinimumChargeKey, Stream<CompleteBillLine>> billMinimumChargeKeys = bill.fold(
        HashMap::empty,
        this::getBillMinimumChargeKeys
    );
    return pendingMinimumChargesAsMap
        .map((key, pendingMinimumCharge) -> Tuple.of(minimumCharges.get(key).get(),
            computePendingMinimumCharge(pendingMinimumCharge, minimumCharges.get(key).get(), billMinimumChargeKeys.get(key)))
        );
  }

  private boolean shouldComplete(PendingMinimumCharge pendingMinimumCharge) {
    return logicalDateIsOnOrAfterGivenDate(logicalDate, pendingMinimumCharge.getMinChargeEndDate());
  }

  private boolean isMinChargeApplicable(PendingMinimumCharge pendingMinimumCharge, MinimumCharge minimumCharge) {
    return minimumCharge.getRate().compareTo(pendingMinimumCharge.getApplicableCharges()) > 0;
  }

  private Map<MinimumChargeKey, PendingMinimumCharge> getPendingMinimumChargesAsMap(Iterable<PendingMinimumCharge> pendingMinimumCharges) {
    return Stream.ofAll(pendingMinimumCharges)
        .toMap(pendingMinimumCharge -> Tuple.of(MinimumChargeKey.from(pendingMinimumCharge), pendingMinimumCharge))
        .filterKeys(key -> minimumCharges.get(key).isDefined());
  }

  private Map<MinimumChargeKey, Stream<CompleteBillLine>> getBillMinimumChargeKeys(CompleteBill bill) {
    return Stream.of(bill.getBillLines())
        .groupBy(bl -> new MinimumChargeKey(bill.getLegalCounterpartyId(), bill.getPartyId(), bill.getCurrency()))
        .merge(
            Stream.of(bill.getBillLines())
                .filter(billLine -> !billLine.getBillLinePartyId().equals(bill.getPartyId()))
                .groupBy(bl -> new MinimumChargeKey(bill.getLegalCounterpartyId(), bl.getBillLinePartyId(), bill.getCurrency())))
        .filterKeys(key -> minimumCharges.get(key).isDefined());
  }

  private PendingMinimumCharge computePendingMinimumCharge(PendingMinimumCharge currentPendingMinimumCharge,
      MinimumCharge minimumCharge, Option<Stream<CompleteBillLine>> billLinesOption) {

    PendingMinimumCharge pendingMinimumCharge = currentPendingMinimumCharge
        .withMinChargeStartDate(minimumCharge.getCycle().getStartDate())
        .withMinChargeEndDate(minimumCharge.getCycle().getEndDate())
        .withMinChargeType(minimumCharge.getRateType())
        .withBillDate(minimumCharge.getCycle().getEndDate());

    //consider only premium and acquired charges
    Option<BigDecimal> sum = billLinesOption.map(
        billLines -> billLines.filter(bl -> PRODUCT_CLASSES.contains(bl.getProductClass()))
            .flatMap(bl -> Stream.of(bl.getBillLineCalculations()))
            .map(CompleteBillLineCalculation::getAmount)
            .fold(BigDecimal.ZERO, BigDecimal::add)
    );

    BigDecimal applicableCharges = pendingMinimumCharge.getApplicableCharges().add(sum.getOrElse(BigDecimal.ZERO));

    return pendingMinimumCharge.withApplicableCharges(applicableCharges);
  }

  private static boolean logicalDateIsOnOrAfterGivenDate(LocalDate logicalDate, LocalDate date) {
    return logicalDate.isEqual(date) || logicalDate.isAfter(date);
  }


  private BigDecimal computeAmount(BigDecimal minChargeRate, BigDecimal applicableCharges){
    return minChargeRate.min(minChargeRate.subtract(applicableCharges));
  }
}