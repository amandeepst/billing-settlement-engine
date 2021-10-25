package com.worldpay.pms.bse.engine.data;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;

import com.worldpay.pms.bse.domain.model.tax.Country;
import com.worldpay.pms.bse.domain.model.tax.TaxRate;
import com.worldpay.pms.bse.domain.model.tax.TaxRule;
import com.worldpay.pms.utils.Strings;
import io.vavr.collection.Stream;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.Objects;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CsvStaticDataRepositoryTest {

  private static final Date LOGICAL_DATE = Date.valueOf("2021-03-04");

  private CsvStaticDataRepository csvStaticDataRepository;

  @BeforeEach
  void setUp() {
    csvStaticDataRepository = new CsvStaticDataRepository(LOGICAL_DATE);
  }

  @Test
  void canReadCountries() {
    int expectedCount = 251;

    Iterable<Country> countries = csvStaticDataRepository.getCountries();

    assertThat(countries, iterableWithSize(expectedCount));
    assertThat(Stream.ofAll(countries).count(country ->
            Strings.isNotNullOrEmptyOrWhitespace(country.getCountryId()) &&
                Strings.isNotNullOrEmptyOrWhitespace(country.getCurrencyId()) &&
                Strings.isNotNullOrEmptyOrWhitespace(country.getName()) &&
                Strings.isNotNullOrEmptyOrWhitespace(country.getEEEAFlag())),
        is(expectedCount));
    // Vatican has null eu geographic code
    assertThat(Stream.ofAll(countries)
        .count(country -> Strings.isNotNullOrEmptyOrWhitespace(country.getEuGeographicCode())), is(250));
    assertThat(countries, hasItems(new Country("GBR", "GBP", "United Kingdom", "NON-EU", null, "Y")));
  }

  @Test
  void canReadTaxRules() {
    int expectedCount = 86;

    Iterable<TaxRule> taxRules = csvStaticDataRepository.getTaxRules();
    assertThat(taxRules, iterableWithSize(expectedCount));
    assertThat(Stream.ofAll(taxRules).count(taxRule ->
            Strings.isNotNullOrEmptyOrWhitespace(taxRule.getLegalCounterPartyId()) &&
                Strings.isNotNullOrEmptyOrWhitespace(taxRule.getLegalCounterPartyCountry()) &&
                Strings.isNotNullOrEmptyOrWhitespace(taxRule.getLegalCounterPartyCurrency()) &&
                Strings.isNotNullOrEmptyOrWhitespace(taxRule.getMerchantRegion()) &&
                Strings.isNotNullOrEmptyOrWhitespace(taxRule.getMerchantTaxRegistered()) &&
                Strings.isNotNullOrEmptyOrWhitespace(taxRule.getTaxAuthority()) &&
                Strings.isNotNullOrEmptyOrWhitespace(taxRule.getTaxType()) &&
                Strings.isNotNullOrEmptyOrWhitespace(taxRule.getTaxStatusType()) &&
                Strings.isNotNullOrEmptyOrWhitespace(taxRule.getReverseCharge()) &&
                Strings.isNotNullOrEmptyOrWhitespace(taxRule.getLegalCounterPartyTaxRegNumber())),
        is(expectedCount));
    assertThat(taxRules,
        hasItems(taxRule("PO1100000001", null, "INTRA-EU", "Y", "GBR", "GBP", "HMRC", "VAT", "TX_S_OOS", "N", "GB991280207")));
  }

  @Test
  void canReadTaxRates() {
    int expectedCount = 41;

    Iterable<TaxRate> taxRates = csvStaticDataRepository.getTaxRates();
    assertThat(taxRates, iterableWithSize(expectedCount));
    assertThat(Stream.ofAll(taxRates).count(taxRate ->
            Strings.isNotNullOrEmptyOrWhitespace(taxRate.getTaxStatusType()) &&
                Strings.isNotNullOrEmptyOrWhitespace(taxRate.getTaxStatusValue()) &&
                Strings.isNotNullOrEmptyOrWhitespace(taxRate.getTaxStatusCode()) &&
                Strings.isNotNullOrEmptyOrWhitespace(taxRate.getTaxStatusDescription()) &&
                !Objects.isNull(taxRate.getTaxRate())),
        is(expectedCount));
    assertThat(taxRates, hasItems(new TaxRate("TX_S_NL", "STANDARD-S", "S", "S Standard", BigDecimal.valueOf(21))));
  }

  private static TaxRule taxRule(String lcp,
      String merchantCountry,
      String merchantRegion,
      String merchantTaxRegistered,
      String lcpCountry,
      String lcpCurrency,
      String taxAuthority,
      String taxType,
      String taxStatusType,
      String reverseCharge,
      String lcpTaxRegNumber) {
    return new TaxRule(lcp,
        lcpCountry,
        lcpCurrency,
        merchantCountry,
        merchantRegion,
        merchantTaxRegistered,
        taxAuthority,
        taxType,
        taxStatusType,
        reverseCharge,
        lcpTaxRegNumber);
  }
}