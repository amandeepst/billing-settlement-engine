package com.worldpay.pms.bse.domain.validation;

import static com.worldpay.pms.bse.domain.model.BillableItem.STANDARD_BILLABLE_ITEM;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.worldpay.pms.bse.domain.model.BillableItem;
import com.worldpay.pms.bse.domain.model.BillableItemServiceQuantity;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.val;
import org.junit.jupiter.api.Test;

class DefaultValidationServiceTest {

  private static final DefaultValidationService validationService = new DefaultValidationService();

  @Test
  void whenMissingAdhocFlagThenReturnEorrorForIt() {
    BillableItem billableItem = getTestBillableItemNullAdhocFlag();
    Validation<Seq<DomainError>, BillableItem> validation = validationService.apply(billableItem);

    assertThatListContainsNullOrEmptyErrorOnFields(validation, "ADHOC_BILL_FLG");
  }

  @Test
  void whenMissingQtyThenReturnErrorForIt() {
    BillableItem billableItem = getTestBillableItemWithServiceQuantity(getBillableItemServiceQuantity(getEmptyServiceQuantities()));
    Validation<Seq<DomainError>, BillableItem> validation = validationService.apply(billableItem);

    assertThatListContainsNullOrEmptyErrorOnFields(validation, "QTY");
  }

  @Test
  void whenNullQtyThenReturnErrorForIt() {
    BillableItem billableItem = getTestBillableItemWithServiceQuantity(getBillableItemServiceQuantity(null));
    Validation<Seq<DomainError>, BillableItem> validation = validationService.apply(billableItem);

    assertThatListContainsNullOrEmptyErrorOnFields(validation, "QTY");
  }

  @Test
  void whenMissingAccruedDateThenReturnErrorForIt() {
    BillableItem billableItem = getTestBillableItemEmptyAccruedDate();
    Validation<Seq<DomainError>, BillableItem> validation = validationService.apply(billableItem);

    assertThatListContainsNullOrEmptyErrorOnFields(validation, "ACCRUED_DT");
  }

  @Test
  void whenMultipleMissingFieldsReturnErrorsForThem() {
    BillableItem billableItem = getTestBillableItemNullAdhocFlag();
    Validation<Seq<DomainError>, BillableItem> validation = validationService.apply(billableItem);

    assertThatListContainsNullOrEmptyErrorOnFields(
        validation,
        "ADHOC_BILL_FLG", "QTY", "FASTEST_PAYMENT_FLG", "IND_PAYMENT_FLG", "PAY_NARRATIVE", "REL_RESERVE_FLG", "REL_WAF_FLG");
  }

  @Test
  void whenCompleteRecordThenNoErrors() {
    Validation<Seq<DomainError>, BillableItem> validation = validationService.apply(standardBillableItem());

    assertThat(validation.isValid(), is(true));
  }

  @Test
  void whenStandardFieldMissingThenThrowOneErrorForIt() {
    Validation<Seq<DomainError>, BillableItem> validation = validationService.apply(standardBillableItemWithNullPriceLine());

    assertThatListContainsNullOrEmptyErrorOnFields(validation, "PRICE_ASGN_ID");
    assertThat(validation.getError().length(), is(1));
  }

  @Test
  void whenLineAmountFieldMissingThenThrowErrorForIt() {
    Validation<Seq<DomainError>, BillableItem> validation = validationService.apply(standardBillableItemWithNullLineAmount());

    assertThatListContainsNullOrEmptyErrorOnFields(validation, "PRECISE_CHARGE_AMOUNT");
    assertThat(validation.getError().length(), is(1));
  }

  @Test
  void whenLineWhitespaceRateTypeFieldThenThrowErrorForRateType() {
    Validation<Seq<DomainError>, BillableItem> validation = validationService.apply(standardBillableItemWithWhitespaceRateType());

    assertThatListContainsNullOrEmptyErrorOnFields(validation, "RATE_TYPE");
    assertThat(validation.getError().length(), is(1));
  }

  @Test
  void whenLineRateTypeFieldIsNullThenThrowErrorForRateValue() {
    Validation<Seq<DomainError>, BillableItem> validation = validationService.apply(standardBillableItemWithNullRateValue());

    assertThatListContainsNullOrEmptyErrorOnFields(validation, "RATE");
    assertThat(validation.getError().length(), is(1));
  }

  private void assertThatListContainsNullOrEmptyErrorOnFields(Validation<Seq<DomainError>, BillableItem> validation, String ... strings){
    assertThat(validation.isInvalid(), is(true));
    assertThat(validation.getError(), is(notNullValue()));
    val errors = validation.getError();
    val searchedErrors = Arrays.stream(strings)
                               .flatMap(item -> Stream.of(
                                   "Unexpected null or empty field `" + item + "`",
                                   "Unexpected null field `" + item + "`"))
                               .collect(Collectors.toList());
    assertThat(errors.filter(domainError -> domainError.getCode().equals("NULL_OR_EMPTY") || domainError.getCode().equals("NULL")).length(),
               greaterThanOrEqualTo(strings.length));
    assertThat(
        errors.filter(domainError -> domainError.getCode().equals("NULL_OR_EMPTY") || domainError.getCode().equals("NULL"))
              .map(DomainError::getMessage)
              .filter(searchedErrors::contains)
              .length(),
        is(strings.length));
  }

  private BillableItem getTestBillableItemWithServiceQuantity(BillableItemServiceQuantity billableItemServiceQuantity) {
    return TestBillableItem.builder()
        .billableItemId("testBillableItem")
        .billingCurrency("EUR")
        .currencyFromScheme("GBP")
        .priceCurrency("GBP")
        .fundingCurrency("EUR")
        .accruedDate(Date.valueOf(LocalDate.now()))
        .productId("MOVRPAY")
        .priceLineId("124")
        .legalCounterparty("00001")
        .granularity("gran")
        .granularityKeyValue("gran|kv")
        .settlementLevelType("SLT")
        .adhocBillFlag("Y")
        .aggregationHash("1245")
        .subAccountId("123456")
        .billableItemServiceQuantity(billableItemServiceQuantity)
        .build();
  }

  private BillableItem getTestBillableItemEmptyAccruedDate() {
    return TestBillableItem.builder()
        .billableItemId("testBillableItem")
        .billingCurrency("EUR")
        .currencyFromScheme("GBP")
        .priceCurrency("GBP")
        .fundingCurrency("EUR")
        .accruedDate(null)
        .productId("MOVRPAY")
        .priceLineId("124")
        .legalCounterparty("00001")
        .granularity("gran")
        .granularityKeyValue("gran|kv")
        .settlementLevelType("SLT")
        .adhocBillFlag("Y")
        .aggregationHash("1245")
        .subAccountId("123456")
        .billableItemServiceQuantity(getBillableItemServiceQuantity(getCorrectServiceQuantities()))
        .build();
  }

  private BillableItem getTestBillableItemNullAdhocFlag() {
    return TestBillableItem.builder()
        .billableItemId("testBillableItem")
        .billingCurrency("EUR")
        .currencyFromScheme("GBP")
        .priceCurrency("GBP")
        .fundingCurrency("EUR")
        .accruedDate(Date.valueOf(LocalDate.now()))
        .productId("MOVRPAY")
        .priceLineId("124")
        .legalCounterparty("00001")
        .granularity("gran")
        .granularityKeyValue("gran|kv")
        .settlementLevelType("SLT")
        .adhocBillFlag(null)
        .aggregationHash("1245")
        .subAccountId("123456")
        .build();
  }

  private static TestBillableItem standardBillableItemWithNullPriceLine() {
    return standardBillableItem(null);
  }

  private static TestBillableItem standardBillableItemWithNullLineAmount() {
    return standardBillableItem("9214474800", getBillableItemLineWithNullAmount());
  }

  private static TestBillableItem standardBillableItemWithWhitespaceRateType() {
    return standardBillableItem("9214474800", getBillableItemLineWithWhitespaceRateType());
  }

  private static TestBillableItem standardBillableItemWithNullRateValue() {
    return standardBillableItem("9214474800", getBillableItemLineWithNullRateValue());
  }

  private static TestBillableItem standardBillableItem() {
    return standardBillableItem("9214474800", getCorrectBillableItemLine());
  }

  private static TestBillableItem standardBillableItem(String priceLineId, TestBillableItemLine... billableItemLines) {
    return TestBillableItem.builder()
        .billableItemId("6df0e527-54e3-4f01-89a8-daaaa245848d")
        .billableItemType(STANDARD_BILLABLE_ITEM)
        .subAccountId("0300301165")
        .legalCounterparty("00018")
        .accruedDate(Date.valueOf("2021-01-26"))
        .priceLineId(priceLineId)
        .settlementLevelType(null)
        .granularityKeyValue(null)
        .billingCurrency("JPY")
        .currencyFromScheme("JPY")
        .fundingCurrency("JPY")
        .priceCurrency("GBP")
        .transactionCurrency("JPY")
        .granularity("2101260300586258")
        .productClass("ACQUIRED")
        .productId("FRAVI")
        .aggregationHash("-1175880790")
        .adhocBillFlag("N")
        .overpaymentIndicator("N")
        .releaseWAFIndicator("N")
        .releaseReserveIndicator("N")
        .fastestSettlementIndicator("N")
        .caseIdentifier("N")
        .individualPaymentIndicator("N")
        .paymentNarrative("N")
        .debtDate(null)
        .debtMigrationType(null)
        .sourceType(null)
        .partitionId(61)
        .ilmDate(Date.valueOf("2021-01-01"))
        .billableItemLines(billableItemLines)
        .billableItemServiceQuantity(getBillableItemServiceQuantity(getCorrectServiceQuantities()))
        .build();
  }

  private static TestBillableItemLine getCorrectBillableItemLine() {
    return TestBillableItemLine.builder()
                              .calculationLineClassification("SETT-CTL")
                              .amount(BigDecimal.valueOf((double) 1774))
                              .calculationLineType("FND_AMTM")
                              .rateType("MSC_PI")
                              .rateValue(BigDecimal.valueOf((double) 1))
                              .partitionId(61)
                              .build();
  }

  private static TestBillableItemLine getBillableItemLineWithNullAmount() {
    return  getBillableItemLineComplete(null, BigDecimal.valueOf((double) 1), "MSC_PI");
  }

  private static TestBillableItemLine getBillableItemLineWithNullRateValue() {
    return  getBillableItemLineComplete(BigDecimal.valueOf((double) 1774), null, "MSC_PI");
  }

  private static TestBillableItemLine getBillableItemLineWithWhitespaceRateType() {
    return  getBillableItemLineComplete(BigDecimal.valueOf((double) 1774), BigDecimal.valueOf((double) 1), "      ");
  }

  private static TestBillableItemLine getBillableItemLineComplete(BigDecimal amount, BigDecimal rateValue, String rateType) {
    return TestBillableItemLine.builder()
                              .calculationLineClassification("SETT-CTL")
                              .amount(amount)
                              .calculationLineType("FND_AMTM")
                              .rateType(rateType)
                              .rateValue(rateValue)
                              .partitionId(61)
                              .build();
  }

  private static TestBillableItemServiceQuantity getBillableItemServiceQuantity(Map<String, BigDecimal> serviceQuantities) {
    return TestBillableItemServiceQuantity.builder()
                                         .rateSchedule("FUNDCQPR")
                                         .serviceQuantities(serviceQuantities)
                                         .build();
  }

  private static Map<String, BigDecimal> getCorrectServiceQuantities() {
    Map<String, BigDecimal> serviceQuantities = new HashMap<>(1);
    serviceQuantities.put("TXN_VOL", BigDecimal.valueOf((double) 2));
    return serviceQuantities;
  }

  private static Map<String, BigDecimal> getEmptyServiceQuantities() {
    Map<String, BigDecimal> serviceQuantities = new HashMap<>(1);
    serviceQuantities.put("TXN_AMT", BigDecimal.valueOf((double) 2));
    return serviceQuantities;
  }

}