package com.worldpay.pms.bse.domain;

import static com.worldpay.pms.bse.domain.account.DefaultAccountDeterminationServiceTest.BILLING_ACCOUNT;
import static com.worldpay.pms.bse.domain.account.DefaultAccountDeterminationServiceTest.BILLING_ACCOUNT_CHRG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.worldpay.pms.bse.domain.account.AccountDeterminationService;
import com.worldpay.pms.bse.domain.account.BillingAccount;
import com.worldpay.pms.bse.domain.account.BillingAccount.BillingAccountBuilder;
import com.worldpay.pms.bse.domain.account.BillingCycle;
import com.worldpay.pms.bse.domain.model.BillableItem;
import com.worldpay.pms.bse.domain.validation.DefaultValidationService;
import com.worldpay.pms.bse.domain.validation.DefaultCalculationCheckService;
import com.worldpay.pms.bse.domain.validation.TestBillableItem;
import com.worldpay.pms.bse.domain.validation.TestBillableItemLine;
import com.worldpay.pms.bse.domain.validation.TestBillableItemServiceQuantity;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.control.Validation;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import lombok.val;
import org.junit.jupiter.api.Test;

class DefaultBillableItemProcessingServiceTest {

  @Test
  void whenUsingDefaultValidationServiceThenBillableItemValidationWorks() {
    DefaultBillableItemProcessingService defaultBillableItemProcessingService = getDefaultBillableItemProcessingService(BILLING_ACCOUNT);

    assertThat(defaultBillableItemProcessingService.process(getIncompleteTestBillableItem()), is(notNullValue()));
  }

  @Test
  void whenUsingIncompleteRecordThenValidationFails() {
    DefaultBillableItemProcessingService defaultBillableItemProcessingService = getDefaultBillableItemProcessingService(BILLING_ACCOUNT);

    val validation = defaultBillableItemProcessingService.process(getIncompleteTestBillableItem());
    assertThat(validation.isInvalid(), is(true));
  }

  @Test
  void whenUsingCompleteRecordThenValidationPasses() {
    DefaultBillableItemProcessingService defaultBillableItemProcessingService = getDefaultBillableItemProcessingService(BILLING_ACCOUNT);
    TestBillableItem testBillableItem = getCompleteStandardBillableItem();

    val validation = defaultBillableItemProcessingService.process(testBillableItem);
    assertThat(validation.isValid(), is(true));
  }

  @Test
  void whenCorrectStandardLineThenFlagIsTrue() {
    DefaultBillableItemProcessingService defaultBillableItemProcessingService = getDefaultBillableItemProcessingService(BILLING_ACCOUNT_CHRG);
    BillableItem testBillableItem = TestBillableItem
        .getStandardBillableItem(TestBillableItemServiceQuantity.getCorrectSvcQty(), TestBillableItemLine.getBillableItemLines());

    val validation = defaultBillableItemProcessingService.process(testBillableItem);
    assertThat(validation.get().isCalculationCorrect(), is(true));
  }

  @Test
  void whenIncorrectStandardLineThenFlagIsFalse() {
    DefaultBillableItemProcessingService defaultBillableItemProcessingService = getDefaultBillableItemProcessingService(BILLING_ACCOUNT_CHRG);
    BillableItem testBillableItem = TestBillableItem
        .getStandardBillableItem(TestBillableItemServiceQuantity.getBadChargingFxSvcQty(), TestBillableItemLine.getBillableItemLines());

    val validation = defaultBillableItemProcessingService.process(testBillableItem);
    assertThat(validation.get().isCalculationCorrect(), is(false));
  }

  @Test
  void whenMiscItemLineThenFlagIsTrue() {
    DefaultBillableItemProcessingService defaultBillableItemProcessingService = getDefaultBillableItemProcessingService(BILLING_ACCOUNT_CHRG);
    BillableItem testBillableItem = TestBillableItem.getMiscBillableItem(TestBillableItemServiceQuantity.getCorrectSvcQty(),TestBillableItemLine.getBillableItemLines());

    val validation = defaultBillableItemProcessingService.process(testBillableItem);
    assertThat(validation.get().isCalculationCorrect(), is(true));
  }


  private static DefaultBillableItemProcessingService getDefaultBillableItemProcessingService(Supplier<BillingAccountBuilder> accountSupplier) {
    return new DefaultBillableItemProcessingService(
        new DefaultValidationService(),
        new DefaultCalculationCheckService(),
        getAccountDeterminationService(accountSupplier));
  }

  private static AccountDeterminationService getAccountDeterminationService(Supplier<BillingAccountBuilder> accountSupplier) {
    return new AccountDeterminationService() {
          @Override
          public Validation<DomainError, BillingAccount> findAccount(AccountKey accountKey) {
            return Validation.valid(accountSupplier.get().subAccountId("1").build());
          }

          @Override
          public Validation<DomainError, BillingCycle> getBillingCycleForAccountId(String accountId,
                                                                                   LocalDate date) {
            return null;
          }

          @Override
          public Validation<DomainError, String> getProcessingGroupForAccountId(String accountId) {
            return null;
          }

          @Override
          public Validation<DomainError, String> getBillingCycleCodeForAccountId(String accountId) {
            return null;
          }

      @Override
      public Validation<DomainError, BillingAccount> getChargingAccountByPartyId(String partyId, String legalCounterParty, String currency) {
        return null;
      }
    };
  }


  private static BillableItem getIncompleteTestBillableItem() {

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
        .build();
  }

  public static TestBillableItem getCompleteStandardBillableItem() {
    return standardBillableItem("9214474800", billableItemLine());
  }

  private static TestBillableItem standardBillableItem(String priceLineId, TestBillableItemLine... billableItemLines) {
    return TestBillableItem.builder()
                           .billableItemId("6df0e527-54e3-4f01-89a8-daaaa245848d")
                           .subAccountId("0300301165")
                           .legalCounterparty("00018")
                           .accruedDate(Date.valueOf("2021-01-26"))
                           .priceLineId(priceLineId)//.priceLineId("9214474800")
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
                           .sourceType("STANDARD_BILLABLE_ITEM")
                           .partitionId(61)
                           .ilmDate(Date.valueOf("2021-01-01"))
                           .billableItemLines(billableItemLines)
                           .billableItemServiceQuantity(billableItemServiceQuantity())
                           .build();
  }

  private static TestBillableItemServiceQuantity billableItemServiceQuantity() {
    Map<String, BigDecimal> serviceQuantities = getServiceQuantities();

    return TestBillableItemServiceQuantity.builder()
                                          .rateSchedule("FUNDCQPR")
                                          .serviceQuantities(serviceQuantities)
                                          .build();
  }

  private static TestBillableItemLine billableItemLine() {
    return TestBillableItemLine.builder()
                               .calculationLineClassification("SETT-CTL")
                               .amount(BigDecimal.valueOf((double) 1774))
                               .calculationLineType("FND_AMTM")
                               .rateType("MSC_PI")
                               .rateValue(BigDecimal.valueOf((double) 1))
                               .partitionId(61)
                               .build();
  }

  private static Map<String, BigDecimal> getServiceQuantities() {
    Map<String, BigDecimal> serviceQuantities = new HashMap<>(1);
    serviceQuantities.put("TXN_VOL", BigDecimal.valueOf((double) 2));
    return serviceQuantities;
  }

}