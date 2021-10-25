package com.worldpay.pms.bse.domain.tax;

import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.LCP_01;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.PARTIES;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.PARTY_IRL_INT_EU_Y;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.RATE_IND_21;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.RATE_IND_22;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.RATE_IND_23;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.RATE_IND_24;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.RATE_ROI_32;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.RATE_ROI_34;
import static com.worldpay.pms.bse.domain.tax.TaxRelatedSamples.RULE_01_NUL_NON_EC_Y;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.worldpay.pms.bse.domain.DefaultRoundingService;
import com.worldpay.pms.bse.domain.RoundingService;
import com.worldpay.pms.bse.domain.common.BillLineDomainError;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTaxDetail;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBill;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLine;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLineCalculation;
import com.worldpay.pms.bse.domain.model.tax.TaxRate;
import com.worldpay.pms.bse.domain.model.tax.TaxRule;
import com.worldpay.pms.bse.domain.staticdata.Currency;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;
import java.math.BigDecimal;
import lombok.NonNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DefaultTaxCalculationServiceTest {

  private static final CompleteBillLineCalculation BILL_LINE_CALC_BASE = CompleteBillLineCalculation.builder()
      .billLineCalcId("xyz")
      .amount(BigDecimal.ZERO)
      .build();

  private static final CompleteBillLine BILL_LINE_BASE = CompleteBillLine.builder()
      .billLineId("999")
      .productIdentifier("prod_id_1")
      .billLineCalculations(new CompleteBillLineCalculation[]{})
      .build();

  private static final CompleteBill BILL = CompleteBill.builder()
      .billId("bill_id_1")
      .partyId("PO1111333333").legalCounterpartyId(LCP_01)
      .billLines(new CompleteBillLine[]{BILL_LINE_BASE})
      .currency("EUR")
      .build();

  private static final String COULD_NOT_DET_TAX_RATE_CODE = "COULD_NOT_DET_TAX_RATE";
  private static final String COULD_NOT_DET_TAX_RULE_CODE = "COULD_NOT_DET_TAX_RULE";

  private TaxRuleDeterminationService taxRuleDeterminationService;
  private TaxRateDeterminationService taxRateDeterminationService;
  private DefaultTaxCalculationService taxCalculationService;
  private static RoundingService roundingService;

  @BeforeAll
  static void init() {
    roundingService = new DefaultRoundingService(List.of(
        new Currency("EUR", (short) 5),
        new Currency("USD", (short) 6),
        new Currency("RON", (short) 6)
    ));
  }

  @Test
  @DisplayName("When multiple bill lines with same tax status are processed "
      + "then they will appear aggregated in the same BillTaxDetail")
  void testMultipleBillLinesThatWillBeAggregatedInSameBillTaxDetail() {
    taxRuleDeterminationService = pB -> Validation.valid(RULE_01_NUL_NON_EC_Y);
    taxRateDeterminationService = (billLine, taxRule) -> {
      switch (Integer.parseInt(billLine.getBillLineId()) % 4) {
        case 0:
          return Validation.valid(RATE_IND_21);
        case 1:
          return Validation.valid(RATE_IND_22);
        case 2:
          return Validation.valid(RATE_IND_23);
        default:
          return Validation.valid(RATE_IND_24);
      }
    };
    taxCalculationService = new DefaultTaxCalculationService(taxRuleDeterminationService, taxRateDeterminationService, PARTIES,
        roundingService);

    BillTax billTax = taxCalculationService.calculate(getBillWithAmounts()).get();
    assertThat(billTax, is(notNullValue()));
    List<BillTaxDetail> billTaxDetails = List.of(billTax.getBillTaxDetails());
    assertThat(billTaxDetails.size(), is(equalTo(4)));

    assertBillTaxDetailsAreCorrectlyComputed(billTaxDetails, RATE_IND_21, "20.00005", "0.20000");
    assertBillTaxDetailsAreCorrectlyComputed(billTaxDetails, RATE_IND_22, "55.00015", "1.10000");
    assertBillTaxDetailsAreCorrectlyComputed(billTaxDetails, RATE_IND_23, "150.00020", "18.00002");
    assertBillTaxDetailsAreCorrectlyComputed(billTaxDetails, RATE_IND_24, "170.00020", "0.00000");

    // verify the party look-up for the merchant tax reg number
    assertThat(billTax.getMerchantTaxRegistrationNumber(), is(equalTo(PARTY_IRL_INT_EU_Y.getMerchantTaxRegistration())));
  }

  @Test
  @DisplayName("When tax calculation fails for at least one bill line, then return invalid")
  void whenTaxCalculationFailsForAtLeastOneButNotAllBillLinesThenReturnInvalid() {
    taxRuleDeterminationService = pB -> Validation.valid(RULE_01_NUL_NON_EC_Y);
    taxRateDeterminationService = (billLine, taxRule) -> "1".equals(billLine.getBillLineId()) ?
        Validation.valid(RATE_IND_21) : getInvalidTaxRate(billLine.getBillLineId());
    taxCalculationService = new DefaultTaxCalculationService(taxRuleDeterminationService, taxRateDeterminationService, PARTIES,
        roundingService);

    Validation<Seq<BillLineDomainError>, BillTax> billTax = taxCalculationService.calculate(getBillWithAmounts());
    assertThat(billTax.isInvalid(), is(true));
    assertThat(billTax.getError().size(), is(6));
    assertThat(billTax.getError().get(0).getDomainError().getCode(), equalTo(COULD_NOT_DET_TAX_RATE_CODE));
  }

  @Test
  @DisplayName("When TaxRuleDeterminationService returns an error, "
      + "then no BillTax or BillTaxDetails are generated and the tax result will be an error")
  void testTaxCalculationWhenTaxRuleDetServiceReturnsError() {
    taxRuleDeterminationService = alwaysFailTaxRuleDeterminationService();
    taxRateDeterminationService = new MockedTaxRateDeterminationService(Validation.valid(RATE_ROI_34));
    taxCalculationService = new DefaultTaxCalculationService(taxRuleDeterminationService, taxRateDeterminationService, PARTIES,
        roundingService);

    Validation<Seq<BillLineDomainError>, BillTax> result = taxCalculationService.calculate(getBillWithAmounts());

    assertTrue(result.isInvalid());
    Seq<BillLineDomainError> errors = result.getError();
    assertThat(errors.size(), is(equalTo(1)));
    assertThat(errors.get(0).getDomainError().getCode(), equalTo(COULD_NOT_DET_TAX_RULE_CODE));
  }

  @Test
  @DisplayName("When TaxRateDeterminationService returns an error (for one or more bill lines), "
      + "then no BillTax or BillTaxDetails are generated and the tax result will be an sequence of errors")
  void testTaxCalculationWhenTaxRateDetServiceReturnsError() {
    taxRuleDeterminationService = bill -> Validation.valid(RULE_01_NUL_NON_EC_Y);
    taxRateDeterminationService = (billLine, taxRule) -> {
      if (Integer.parseInt(billLine.getBillLineId()) % 2 == 0) {
        return Validation.valid(RATE_ROI_32);
      }
      return getInvalidTaxRate(billLine.getBillLineId());
    };
    taxCalculationService = new DefaultTaxCalculationService(taxRuleDeterminationService, taxRateDeterminationService, PARTIES,
        roundingService);

    Validation<Seq<BillLineDomainError>, BillTax> result = taxCalculationService.calculate(getBillWithAmounts());

    assertTrue(result.isInvalid());
    Seq<BillLineDomainError> errors = result.getError();
    assertThat(errors.size(), is(equalTo(4)));
    assertThat(errors.get(0).getDomainError().getCode(), equalTo(COULD_NOT_DET_TAX_RATE_CODE));
  }

  @Test
  @DisplayName("When TaxRuleDeterminationService returns an error and "
      + "TaxRateDeterminationService returns an error (for one or more bill lines), "
      + "then no BillTax or BillTaxDetails are generated and the tax result will be an tax rule related error")
  void testTaxRateAndTaxRuleReturnInvalid() {
    taxRuleDeterminationService = alwaysFailTaxRuleDeterminationService();
    taxRateDeterminationService = (billLine, taxRule) -> getInvalidTaxRate(billLine.getBillLineId());
    taxCalculationService = new DefaultTaxCalculationService(taxRuleDeterminationService, taxRateDeterminationService, PARTIES,
        roundingService);

    Validation<Seq<BillLineDomainError>, BillTax> result = taxCalculationService.calculate(getBillWithAmounts());

    assertTrue(result.isInvalid());
    Seq<BillLineDomainError> errors = result.getError();
    assertThat(errors.size(), is(equalTo(1)));
    assertThat(errors.get(0).getDomainError().getCode(), equalTo(COULD_NOT_DET_TAX_RULE_CODE));
  }

  ///
  private void assertBillTaxDetailsAreCorrectlyComputed(List<BillTaxDetail> billTaxDetails, TaxRate taxRate, String expectedNetAmount,
      String expectedTaxAmount) {
    BillTaxDetail billTaxDetail = billTaxDetails.filter(billTaxDet -> billTaxDet.getTaxStatus().equals(taxRate.getTaxStatusCode()))
        .getOrElse((BillTaxDetail) null);

    assertThat(billTaxDetail, is(notNullValue()));
    assertThat(billTaxDetail.getNetAmount().toPlainString(), is(expectedNetAmount));
    assertThat(billTaxDetail.getTaxAmount().toPlainString(), is(expectedTaxAmount));
  }

  private Validation<DomainError, TaxRate> getInvalidTaxRate(String billLineId) {
    return Validation.invalid(
        DomainError.of(COULD_NOT_DET_TAX_RATE_CODE, "Could not determinate tax rate for bill line with id: " + billLineId));
  }

  private static TaxRuleDeterminationService alwaysFailTaxRuleDeterminationService() {
    return bill -> Validation.invalid(
        DomainError.of(COULD_NOT_DET_TAX_RULE_CODE, "No tax rule matched the bill: " + bill.getBillId()));
  }

  public static class MockedTaxRateDeterminationService implements TaxRateDeterminationService {

    private final Validation<DomainError, TaxRate> mockedResult;

    public MockedTaxRateDeterminationService(Validation<DomainError, TaxRate> mockedResult) {
      this.mockedResult = mockedResult;
    }

    @Override
    public Validation<DomainError, TaxRate> apply(@NonNull CompleteBillLine billLine, @NonNull TaxRule taxRule) {
      return mockedResult;
    }
  }

  private CompleteBill getBillWithAmounts() {
    CompleteBillLine billLine1 = BILL_LINE_BASE.withBillLineId("1").withBillLineCalculations(
        new CompleteBillLineCalculation[]{
            BILL_LINE_CALC_BASE.withBillLineCalcId("1").withAmount(BigDecimal.valueOf(5.00005))});
    CompleteBillLine billLine2 = BILL_LINE_BASE.withBillLineId("2").withBillLineCalculations(
        new CompleteBillLineCalculation[]{
            BILL_LINE_CALC_BASE.withBillLineCalcId("1").withAmount(BigDecimal.valueOf(10.00005)),
            BILL_LINE_CALC_BASE.withBillLineCalcId("2").withAmount(BigDecimal.valueOf(10.00005))});
    CompleteBillLine billLine3 = BILL_LINE_BASE.withBillLineId("3").withBillLineCalculations(
        new CompleteBillLineCalculation[]{
            BILL_LINE_CALC_BASE.withBillLineCalcId("1").withAmount(BigDecimal.valueOf(15.00005)),
            BILL_LINE_CALC_BASE.withBillLineCalcId("3").withAmount(BigDecimal.valueOf(15.00005))});
    CompleteBillLine billLine4 = BILL_LINE_BASE.withBillLineId("4").withBillLineCalculations(
        new CompleteBillLineCalculation[]{
            BILL_LINE_CALC_BASE.withBillLineCalcId("4").withAmount(BigDecimal.valueOf(20.00005))});
    CompleteBillLine billLine5 = BILL_LINE_BASE.withBillLineId("5").withBillLineCalculations(
        new CompleteBillLineCalculation[]{
            BILL_LINE_CALC_BASE.withBillLineCalcId("1").withAmount(BigDecimal.valueOf(25.00005)),
            BILL_LINE_CALC_BASE.withBillLineCalcId("5").withAmount(BigDecimal.valueOf(25.00005))});
    CompleteBillLine billLine6 = BILL_LINE_BASE.withBillLineId("6").withBillLineCalculations(
        new CompleteBillLineCalculation[]{
            BILL_LINE_CALC_BASE.withBillLineCalcId("1").withAmount(BigDecimal.valueOf(100.00005)),
            BILL_LINE_CALC_BASE.withBillLineCalcId("6").withAmount(BigDecimal.valueOf(30.00005))});
    CompleteBillLine billLine7 = BILL_LINE_BASE.withBillLineId("7").withBillLineCalculations(
        new CompleteBillLineCalculation[]{
            BILL_LINE_CALC_BASE.withBillLineCalcId("1").withAmount(BigDecimal.valueOf(105.00005)),
            BILL_LINE_CALC_BASE.withBillLineCalcId("7").withAmount(BigDecimal.valueOf(35.00005))});

    return BILL.withBillLines(new CompleteBillLine[]{billLine1, billLine2, billLine3, billLine4, billLine5, billLine6, billLine7})
        .withPartyId(PARTY_IRL_INT_EU_Y.getPartyId());
  }
}