package com.worldpay.pms.bse.domain.tax;

import com.worldpay.pms.bse.domain.RoundingService;
import com.worldpay.pms.bse.domain.common.BillLineDomainError;
import com.worldpay.pms.bse.domain.common.Utils;
import com.worldpay.pms.bse.domain.model.billtax.BillLineTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTaxDetail;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBill;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLine;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLineCalculation;
import com.worldpay.pms.bse.domain.model.tax.Party;
import com.worldpay.pms.bse.domain.model.tax.TaxRate;
import com.worldpay.pms.bse.domain.model.tax.TaxRule;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Seq;
import io.vavr.collection.Stream;
import io.vavr.control.Validation;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.function.Function;
import lombok.NonNull;

public class DefaultTaxCalculationService implements TaxCalculationService {

  private static final BigDecimal ONE_HUNDRED = BigDecimal.valueOf(100);

  private final TaxRuleDeterminationService taxRuleDeterminationService;
  private final TaxRateDeterminationService taxRateDeterminationService;
  private final Map<String, Party> parties;
  private final RoundingService roundingService;

  public DefaultTaxCalculationService(TaxRuleDeterminationService taxRuleDeterminationService,
      TaxRateDeterminationService taxRateDeterminationService, Iterable<Party> parties, RoundingService roundingService) {
    this.taxRuleDeterminationService = taxRuleDeterminationService;
    this.taxRateDeterminationService = taxRateDeterminationService;
    this.parties = Stream.ofAll(parties).toMap(Party::getPartyId, Function.identity());
    this.roundingService = roundingService;
  }

  @Override
  public Validation<Seq<BillLineDomainError>, BillTax> calculate(@NonNull CompleteBill bill) {
    return taxRuleDeterminationService.apply(bill)
        .mapError(error -> BillLineDomainError.from(error).toSeq())
        .flatMap(taxRule -> getBillLineTaxes(bill, taxRule)
            .map(billLineTaxes -> getBillTax(bill, taxRule, billLineTaxes))
        );
  }

  /**
   * If a tax rate determination fails for at least one bill line, the method will return only the errors, otherwise, it will return the
   * bill line taxes
   */
  private Validation<Seq<BillLineDomainError>, Seq<Tuple2<String, BillLineTax>>> getBillLineTaxes(CompleteBill bill, TaxRule taxRule) {
    return Validation.sequence(
        List.of(bill.getBillLines())
            .map(billLine ->
                taxRateDeterminationService.apply(billLine, taxRule)
                    .map(taxRate -> new Tuple2<>(billLine.getBillLineId(), getBillLineTax(billLine, taxRate, bill.getCurrency())))
                    .mapError(error -> BillLineDomainError.from(billLine.getBillLineId(), error).toSeq())
            ));
  }

  private BillLineTax getBillLineTax(CompleteBillLine billLine, TaxRate taxRate, String currency) {
    BigDecimal taxRatePercent = taxRate.getTaxRate().divide(ONE_HUNDRED);
    BigDecimal totalAmount = List.of(billLine.getBillLineCalculations())
        .map(CompleteBillLineCalculation::getAmount)
        .map(amount -> roundingService.roundAmount(amount, currency))
        .reduce(BigDecimal::add);

    return new BillLineTax(
        taxRate.getTaxStatusCode(),
        taxRate.getTaxRate(),
        taxRate.getTaxStatusDescription(),
        totalAmount,
        totalAmount.multiply(taxRatePercent));
  }

  private BillTax getBillTax(CompleteBill bill, TaxRule taxRule, Seq<Tuple2<String, BillLineTax>> billLineTaxes) {
    String billTaxId = Utils.generateId();
    HashMap<String, BillLineTax> billLineTaxMap = billLineTaxes.toJavaMap(HashMap::new, Tuple2::_1, Tuple2::_2);

    return new BillTax(
        billTaxId,
        bill.getBillId(),
        getMerchantTaxRegistrationNumber(bill),
        taxRule.getLegalCounterPartyTaxRegNumber(),
        taxRule.getTaxType(),
        taxRule.getTaxAuthority(),
        taxRule.getReverseCharge(),
        billLineTaxMap,
        getBillTaxDetails(billTaxId, billLineTaxMap, bill.getCurrency())
    );
  }

  private BillTaxDetail[] getBillTaxDetails(String billTaxId, HashMap<String, BillLineTax> billLineTaxMap, String currency) {
    return List.ofAll(billLineTaxMap.values())
        .groupBy(BillLineTax::getTaxStatus)
        .mapValues(seq -> seq.reduce(BillTax::reduceBillLineTaxes))
        .values()
        .map(billLineTax -> BillTaxDetail.from(billTaxId, billLineTax, currency, roundingService::roundAmount))
        .toJavaArray(BillTaxDetail[]::new);
  }

  private String getMerchantTaxRegistrationNumber(CompleteBill bill) {
    return parties.get(bill.getPartyId())
        .map(Party::getMerchantTaxRegistration)
        .getOrElse((String) null);
  }
}
