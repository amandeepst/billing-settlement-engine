package com.worldpay.pms.bse.domain.tax;

import static com.worldpay.pms.bse.domain.common.ErrorCatalog.NO_TAX_PRODUCT_CHARACTERISTIC_FOUND;
import static com.worldpay.pms.bse.domain.common.ErrorCatalog.NO_TAX_RATE_FOUND;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.PRODUCT_CHARACTERISTICS;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.RATE_ROI_38;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.RULE_01_IRL_INT_EU_Y;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.TAX_RATES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLine;
import com.worldpay.pms.bse.domain.model.tax.TaxRate;
import com.worldpay.pms.bse.domain.model.tax.TaxRule;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.control.Validation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DefaultTaxRateDeterminationServiceTest {

  private DefaultTaxRateDeterminationService taxRateDeterminationService;

  @BeforeEach
  void setUp() {
    taxRateDeterminationService = new DefaultTaxRateDeterminationService(PRODUCT_CHARACTERISTICS, TAX_RATES);
  }

  @DisplayName("When product characteristic and tax rate are found then return tax rate")
  @Test
  void whenProductCharAndTaxRateAreFoundThenReturnTaxRate() {
    assertTaxRateDetermined(billLine("PRMCCC06"), RULE_01_IRL_INT_EU_Y, RATE_ROI_38);
  }

  @DisplayName("When product characteristic is not found then return error")
  @Test
  void whenProductCharIsNotFoundThenReturnError() {
    assertNoTaxRateDetermined(billLine("PRMCCC05"), RULE_01_IRL_INT_EU_Y,
        NO_TAX_PRODUCT_CHARACTERISTIC_FOUND, "No tax product characteristic found for key `[PRMCCC05, TX_S_ROI]`");
  }

  @DisplayName("When tax rate is not found then return error")
  @Test
  void whenTaxRateIsNotFoundThenReturnError() {
    assertNoTaxRateDetermined(billLine("PRMCCC07"), RULE_01_IRL_INT_EU_Y,
        NO_TAX_RATE_FOUND, "No tax rate found for key `[TX_S_ROI, GST STD-S]`");
  }

  private void assertTaxRateDetermined(CompleteBillLine billLine, TaxRule taxRule, TaxRate taxRate) {
    Validation<DomainError, TaxRate> apply = taxRateDeterminationService.apply(billLine, taxRule);
    assertThat(apply.isValid(), is(true));
    assertThat(apply.get(), equalTo(taxRate));
  }

  private void assertNoTaxRateDetermined(CompleteBillLine billLine, TaxRule taxRule, String reason, String errorMessage) {
    Validation<DomainError, TaxRate> apply = taxRateDeterminationService.apply(billLine, taxRule);
    assertThat(apply.isInvalid(), is(true));
    assertThat(apply.getError(), equalTo(DomainError.of(reason, errorMessage)));
  }

  private static CompleteBillLine billLine(String productIdentifier) {
    return CompleteBillLine.builder()
        .productIdentifier(productIdentifier)
        .build();
  }
}