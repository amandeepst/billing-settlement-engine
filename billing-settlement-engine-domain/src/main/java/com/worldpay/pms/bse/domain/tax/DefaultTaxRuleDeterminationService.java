package com.worldpay.pms.bse.domain.tax;

import static com.worldpay.pms.bse.domain.common.ErrorCatalog.noTaxRuleFound;
import static com.worldpay.pms.utils.Strings.isNullOrEmptyOrWhitespace;

import com.worldpay.pms.bse.domain.model.completebill.CompleteBill;
import com.worldpay.pms.bse.domain.model.tax.Party;
import com.worldpay.pms.bse.domain.model.tax.TaxRule;
import com.worldpay.pms.bse.domain.model.tax.TaxRuleLookupKey;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.Function2;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import io.vavr.control.Validation;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.NonNull;

public class DefaultTaxRuleDeterminationService implements TaxRuleDeterminationService {

  private static final
  List<Tuple2<Function<TaxRule, String>, Function2<DefaultTaxRuleDeterminationService, CompleteBill, String>>> NULLABLE_CRITERIA =
      List.of(
          Tuple.of(TaxRule::getMerchantCountry, DefaultTaxRuleDeterminationService::getMerchantCountry)
      );

  private final Map<TaxRuleLookupKey, List<TaxRule>> taxRules;
  private final Map<String, Party> parties;

  public DefaultTaxRuleDeterminationService(Iterable<TaxRule> taxRules, Iterable<Party> parties) {
    this.parties = getPartiesAsMap(parties);
    this.taxRules = getGroupedAndOrderedTaxRules(taxRules);
  }

  @Override
  public Validation<DomainError, TaxRule> apply(@NonNull CompleteBill bill) {
    TaxRuleLookupKey lookupKey = getLookupKey(bill);
    Option<List<TaxRule>> matchedByKey = taxRules.get(lookupKey);
    return matchedByKey
        .flatMap(matchedByKeyTaxRules ->
            // first match among the rules looked up by key should be the most exact one,
            // since the list of rules is ordered from most to least exact rule
            matchedByKeyTaxRules.find(taxRule -> matches(taxRule, bill)))
        .toValidation(() -> noTaxRuleFound(getFullKeyAsString(lookupKey, bill)));
  }

  private Map<String, Party> getPartiesAsMap(Iterable<Party> parties) {
    return Stream.ofAll(parties)
        .toMap(party -> Tuple.of(party.getPartyId(), party));
  }

  private Map<TaxRuleLookupKey, List<TaxRule>> getGroupedAndOrderedTaxRules(Iterable<TaxRule> taxRules) {
    // group by lookup key and order values of a key by the number of null / empty / whitespace nullable criteria fields
    List<Function<TaxRule, String>> nullableCriteriaFields = NULLABLE_CRITERIA.map(Tuple2::_1);
    return Stream.ofAll(taxRules)
        .groupBy(TaxRuleLookupKey::from)
        .mapValues(values ->
            values.toList().sorted(new NullStringFieldsCountComparator<>(nullableCriteriaFields))
        );
  }

  private TaxRuleLookupKey getLookupKey(CompleteBill bill) {
    return new TaxRuleLookupKey(
        getLegalCounterpartyId(bill),
        getMerchantRegion(bill),
        getMerchantTaxRegistered(bill)
    );
  }

  private String getLegalCounterpartyId(CompleteBill bill) {
    return bill.getLegalCounterpartyId();
  }

  private String getMerchantCountry(CompleteBill bill) {
    return parties.get(bill.getPartyId())
        .map(Party::getCountryId)
        .getOrElse((String) null);
  }

  private String getMerchantRegion(CompleteBill bill) {
    return parties.get(bill.getPartyId())
        .map(Party::getMerchantRegion)
        .getOrElse((String) null);
  }

  private String getMerchantTaxRegistered(CompleteBill bill) {
    return parties.get(bill.getPartyId())
        .map(Party::getMerchantTaxRegistered)
        .getOrElse((String) null);
  }

  private boolean matches(TaxRule taxRule, CompleteBill bill) {
    return NULLABLE_CRITERIA.toJavaStream()
        .allMatch(criteria -> isNullOrEmptyOrWhitespace(criteria._1().apply(taxRule)) ||
            criteria._1().apply(taxRule).equals(criteria._2().apply(this, bill)));
  }

  private String getFullKeyAsString(TaxRuleLookupKey lookupKey, CompleteBill bill) {
    return List.of(lookupKey.getLegalCounterPartyId(), lookupKey.getMerchantRegion(), lookupKey.getMerchantTaxRegistered())
        .appendAll(NULLABLE_CRITERIA.map(Tuple2::_2).map(getter -> getter.apply(this, bill)))
        .map(value -> isNullOrEmptyOrWhitespace(value) ? "(NULL)" : value)
        .collect(Collectors.joining(", "));
  }
}