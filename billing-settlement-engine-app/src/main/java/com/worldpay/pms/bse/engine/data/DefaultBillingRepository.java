package com.worldpay.pms.bse.engine.data;

import com.worldpay.pms.bse.domain.account.AccountRepository;
import com.worldpay.pms.bse.domain.account.BillingCycle;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumCharge;
import com.worldpay.pms.bse.domain.model.tax.Country;
import com.worldpay.pms.bse.domain.model.tax.Party;
import com.worldpay.pms.bse.domain.model.tax.ProductCharacteristic;
import com.worldpay.pms.bse.domain.model.tax.TaxRate;
import com.worldpay.pms.bse.domain.model.tax.TaxRule;
import com.worldpay.pms.bse.domain.staticdata.CalculationLineTypeDescription;
import com.worldpay.pms.bse.domain.staticdata.Currency;
import com.worldpay.pms.bse.domain.staticdata.ProductDescription;
import com.worldpay.pms.bse.domain.staticdata.ServiceQuantityDescription;
import com.worldpay.pms.utils.Strings;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import java.sql.Date;
import java.time.LocalDate;
import java.util.function.Function;
import lombok.Getter;

public class DefaultBillingRepository implements BillingRepository {

  private static final short DEFAULT_ROUNDING_SCALE = 2;

  private final Date logicalDate;

  private final JdbcStaticDataRepository staticDataRepository;
  private final CsvStaticDataRepository csvStaticDataRepository;
  private final AccountRepository accountRepository;

  private final Map<String, Country> countries;

  @Getter
  private final Iterable<Currency> currencies;
  private final Map<String, String> productDescriptionMap;
  private final Map<String, String> calcLineTypeDescriptionMap;
  private final Map<String, String> serviceQuantityDescriptionMap;

  private final Map<String, BillingCycle> billingCycleMap;

  public DefaultBillingRepository(Date logicalDate, JdbcStaticDataRepository staticDataRepository,
      CsvStaticDataRepository csvStaticDataRepository, AccountRepository accountRepository) {
    this.logicalDate = logicalDate;
    this.staticDataRepository = staticDataRepository;
    this.csvStaticDataRepository = csvStaticDataRepository;
    this.accountRepository = accountRepository;

    this.countries = getCountries(csvStaticDataRepository.getCountries());

    this.currencies = computeCurrencies();
    this.productDescriptionMap = getProductDescription();
    this.calcLineTypeDescriptionMap = getCalculationLineTypeDescriptionMap();
    this.serviceQuantityDescriptionMap = getServiceQuantityDescription();

    this.billingCycleMap = getBillingCycles();
  }

  @Override
  public String getCalculationTypeBillDisplay(String calculationLineType) {
    return csvStaticDataRepository.getCalculationTypeBillDisplays().getOrElse(calculationLineType, "N");
  }

  @Override
  public Short getRoundingScale(String currency) {
    return Stream.ofAll(currencies)
        .find(c -> c.getCurrencyCode().equals(currency))
        .map(Currency::getRoundingScale)
        .getOrElse(DEFAULT_ROUNDING_SCALE);
  }

  @Override
  public String getProductDescription(String productCode) {
    return productDescriptionMap.get(productCode).getOrElse(() -> null);
  }

  @Override
  public String getCalcLineTypeDescription(String key) {
    return calcLineTypeDescriptionMap.get(key).getOrElse(() -> null);
  }

  @Override
  public String getSvcQtyDescription(String svcQtyCode) {
    return serviceQuantityDescriptionMap.get(svcQtyCode).getOrElse(() -> null);
  }

  @Override
  public Iterable<Party> getParties() {
    return Stream.ofAll(accountRepository.getParties())
        .map(party -> {
          Country country = getCountry(party.getCountryId());
          return party.toBuilder().merchantRegion(country == null ? null : country.getEuGeographicCode())
              .merchantTaxRegistered(Strings.isNullOrEmptyOrWhitespace(party.getMerchantTaxRegistration()) ? "N" : "Y")
              .build();
        });
  }

  @Override
  public Iterable<TaxRule> getTaxRules() {
    return csvStaticDataRepository.getTaxRules();
  }

  @Override
  public Iterable<TaxRate> getTaxRates() {
    return csvStaticDataRepository.getTaxRates();
  }

  @Override
  public Iterable<ProductCharacteristic> getProductCharacteristics() {
    return staticDataRepository.getProductCharacteristics();
  }

  @Override
  public Iterable<MinimumCharge> getMinimumCharges() {
    return Stream.ofAll(staticDataRepository.getMinimumCharge())
        .map(minimumCharge -> minimumCharge.toBuilder()
            .cycle(billingCycleMap.get(minimumCharge.getCycle().getCode()).getOrElse((BillingCycle) null))
            .build());
  }

  private Map<String, Country> getCountries(Iterable<Country> countries) {
    return Stream.ofAll(countries)
        .toMap(Country::getCountryId, Function.identity());
  }

  private Iterable<Currency> computeCurrencies() {
    return staticDataRepository.getDatabaseCurrencies();
  }

  private Map<String, String> getProductDescription() {
    return Stream.ofAll(staticDataRepository.getDatabaseProductDescription())
        .toMap(ProductDescription::getProductCode, ProductDescription::getDescription);
  }

  private Map<String, String> getCalculationLineTypeDescriptionMap() {
    return Stream.ofAll(staticDataRepository.getDatabaseCalculationLineTypeDescriptions())
        .toMap(CalculationLineTypeDescription::getCalculationLineType, CalculationLineTypeDescription::getDescription);
  }

  private Map<String, String> getServiceQuantityDescription() {
    return Stream.ofAll(staticDataRepository.getDatabaseServiceQuantityDescription())
        .toMap(ServiceQuantityDescription::getServiceQuantityCode, ServiceQuantityDescription::getDescription);
  }

  private Country getCountry(String countryId) {
    return countries.get(countryId).getOrElse((Country) null);
  }

  private Map<String, BillingCycle> getBillingCycles() {
    return Stream.ofAll(accountRepository.getBillingCycles())
        .filter(billingCycle -> dateIsOnOrAfterDate(logicalDate.toLocalDate(), billingCycle.getStartDate())
            && dateIsOnOrBeforeDate(logicalDate.toLocalDate(), billingCycle.getEndDate()))
        .toMap(BillingCycle::getCode, Function.identity());
  }

  private static boolean dateIsOnOrAfterDate(LocalDate date1, LocalDate date2) {
    return date1.isEqual(date2) || date1.isAfter(date2);
  }

  private static boolean dateIsOnOrBeforeDate(LocalDate date1, LocalDate date2) {
    return date1.isEqual(date2) || date1.isBefore(date2);
  }
}
