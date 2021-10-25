package com.worldpay.pms.bse.domain.tax;

import static com.worldpay.pms.bse.domain.common.ErrorCatalog.NO_TAX_RULE_FOUND;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.LCP_01;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.LCP_02;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.PARTIES;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.PARTY_AUS_NON_EC_N;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.PARTY_AUS_NON_EC_Y;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.PARTY_BEL_INT_EU_Y;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.PARTY_IRL_INT_EU_N;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.PARTY_IRL_INT_EU_Y;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.PARTY_NLD_INT_EU_N;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.PARTY_NLD_INT_EU_Y;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.PARTY_ROU_INT_EU_N;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.PARTY_ROU_INT_EU_Y;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.RULE_01_IRL_INT_EU_N;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.RULE_01_IRL_INT_EU_Y;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.RULE_01_NUL_INT_EU_N;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.RULE_01_NUL_INT_EU_Y;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.RULE_01_NUL_NON_EC_N;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.RULE_01_NUL_NON_EC_Y;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.RULE_02_NLD_INT_EU_N;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.RULE_02_NLD_INT_EU_Y;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.TAX_RULES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.domain.model.completebill.CompleteBill;
import com.worldpay.pms.bse.domain.model.tax.TaxRule;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.control.Validation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class DefaultTaxRuleDeterminationServiceTest {

  private DefaultTaxRuleDeterminationService taxRuleDeterminationService;

  @BeforeEach
  void setUp() {
    taxRuleDeterminationService = new DefaultTaxRuleDeterminationService(TAX_RULES, PARTIES);
  }

  @DisplayName("When party for Bill partyId does not exist then no rule is determined")
  @Test
  void whenPartyForBillPartyIdDoesNotExistThenNoRuleIsDetermined() {
    assertNoTaxRuleDetermined(bill(LCP_01, "POXXXXXXXXXX"),
        "No tax rule found for key `[PO1100000001, (NULL), (NULL), (NULL)]`");
  }

  @DisplayName("When rule for Bill LCP does not exist then no rule is determined")
  @Test
  void whenRuleForBillLCPDoesNotExistThenNoRuleIsDetermined() {
    assertNoTaxRuleDetermined(bill("POXXXXXXXXXX", PARTY_ROU_INT_EU_Y.getPartyId()),
        "No tax rule found for key `[POXXXXXXXXXX, INTRA-EU, Y, ROU]`");
  }

  @DisplayName("When rule for Bill merchant country does not exist then no rule is determined")
  @Test
  void whenRuleForBillMerchantCountryDoesNotExistThenNoRuleIsDetermined() {
    assertNoTaxRuleDetermined(bill(LCP_02, PARTY_BEL_INT_EU_Y.getPartyId()),
        "No tax rule found for key `[PO1100000002, INTRA-EU, Y, BEL]`");
  }

  @DisplayName("ROU on Bill matches with NULL merchant country when lookup succeeds")
  @Test
  void ROUMerchantCountryOnBillIsMatchedWithNULLMerchantCountryRule_YTaxRegistered() {
    assertTaxRuleDetermined(bill(LCP_01, PARTY_ROU_INT_EU_Y.getPartyId()), RULE_01_NUL_INT_EU_Y);
  }

  @DisplayName("ROU on Bill matches with NULL merchant country when lookup succeeds")
  @Test
  void ROUMerchantCountryOnBillIsMatchedWithNULLMerchantCountryRule_NTaxRegistered() {
    assertTaxRuleDetermined(bill(LCP_01, PARTY_ROU_INT_EU_N.getPartyId()), RULE_01_NUL_INT_EU_N);
  }

  @DisplayName("AUS on Bill matches with NULL merchant country when lookup succeeds")
  @Test
  void AUSMerchantCountryOnBillIsMatchedWithNULLMerchantCountryRule_YTaxRegistered() {
    assertTaxRuleDetermined(bill(LCP_01, PARTY_AUS_NON_EC_Y.getPartyId()), RULE_01_NUL_NON_EC_Y);
  }

  @DisplayName("AUS on Bill matches with NULL merchant country when lookup succeeds")
  @Test
  void AUSMerchantCountryOnBillIsMatchedWithNULLMerchantCountryRule_NTaxRegistered() {
    assertTaxRuleDetermined(bill(LCP_01, PARTY_AUS_NON_EC_N.getPartyId()), RULE_01_NUL_NON_EC_N);
  }

  @DisplayName("IRL on Bill matches with IRL merchant country and not NULL merchant country when lookup succeeds")
  @Test
  void IRLMerchantCountryOnBillIsMatchedWithIRLMerchantCountryRule_YTaxRegistered() {
    assertTaxRuleDetermined(bill(LCP_01, PARTY_IRL_INT_EU_Y.getPartyId()), RULE_01_IRL_INT_EU_Y);
  }

  @DisplayName("IRL on Bill matches with IRL merchant country and not NULL merchant country when lookup succeeds")
  @Test
  void IRLMerchantCountryOnBillIsMatchedWithIRLMerchantCountryRule_NTaxRegistered() {
    assertTaxRuleDetermined(bill(LCP_01, PARTY_IRL_INT_EU_N.getPartyId()), RULE_01_IRL_INT_EU_N);
  }

  @DisplayName("NLD on Bill matches with NLD merchant country when lookup succeeds")
  @Test
  void NLDMerchantCountryOnBillIsMatchedWithNLDMerchantCountryRule_YTaxRegistered() {
    assertTaxRuleDetermined(bill(LCP_02, PARTY_NLD_INT_EU_Y.getPartyId()), RULE_02_NLD_INT_EU_Y);
  }

  @DisplayName("NLD on Bill matches with NLD merchant country when lookup succeeds")
  @Test
  void NLDMerchantCountryOnBillIsMatchedWithNLDMerchantCountryRule_NTaxRegistered() {
    assertTaxRuleDetermined(bill(LCP_02, PARTY_NLD_INT_EU_N.getPartyId()), RULE_02_NLD_INT_EU_N);
  }

  private void assertTaxRuleDetermined(CompleteBill bill, TaxRule taxRule) {
    Validation<DomainError, TaxRule> apply = taxRuleDeterminationService.apply(bill);
    assertThat(apply.isValid(), is(true));
    assertThat(apply.get(), equalTo(taxRule));
  }

  private void assertNoTaxRuleDetermined(CompleteBill bill, String errorMessage) {
    Validation<DomainError, TaxRule> apply = taxRuleDeterminationService.apply(bill);
    assertThat(apply.isInvalid(), is(true));
    assertThat(apply.getError(), equalTo(DomainError.of(NO_TAX_RULE_FOUND, errorMessage)));
  }

  private static CompleteBill bill(String lcp, String partyId) {
    return CompleteBill.builder()
        .legalCounterpartyId(lcp)
        .partyId(partyId)
        .build();
  }
}
