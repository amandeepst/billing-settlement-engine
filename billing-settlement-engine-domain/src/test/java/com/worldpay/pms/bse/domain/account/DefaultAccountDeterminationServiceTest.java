package com.worldpay.pms.bse.domain.account;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountDetails;
import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountKey;
import com.worldpay.pms.bse.domain.account.BillingAccount.BillingAccountBuilder;
import com.worldpay.pms.bse.domain.model.tax.Party;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import io.vavr.control.Validation;
import java.time.LocalDate;
import java.util.Collections;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class DefaultAccountDeterminationServiceTest {

  private static LocalDate DATE = LocalDate.of(2021, 1, 10);

  public final static Supplier<BillingAccountBuilder> BILLING_ACCOUNT = () -> BillingAccount.builder()
      .billingCycle(new BillingCycle("WPMO", LocalDate.of(2021, 1, 1), LocalDate.of(2021, 1,31)))
      .accountId("1234")
      .currency("GBP")
      .accountType("FUND")
      .businessUnit("BU123")
      .legalCounterparty("P000001")
      .childPartyId("P000007")
      .partyId("P00003")
      .subAccountType("FUND");

  public final static Supplier<BillingAccountBuilder> BILLING_ACCOUNT_CHRG = () -> BillingAccount.builder()
      .billingCycle(new BillingCycle("WPMO", LocalDate.of(2021, 1, 1), LocalDate.of(2021, 1,31)))
      .accountId("1234")
      .currency("GBP")
      .accountType("CHRG")
      .businessUnit("BU123")
      .legalCounterparty("P000001")
      .childPartyId("P000007")
      .partyId("P00003")
      .subAccountType("CHRG");

  public final static List<BillingAccount> BILLING_ACCOUNTS = List.of(
      BILLING_ACCOUNT.get().subAccountId("1").build(),
      BILLING_ACCOUNT.get().subAccountId("2").build(),
      BILLING_ACCOUNT.get().subAccountId("2").build(),
      BILLING_ACCOUNT.get().subAccountId("4").billingCycle(new BillingCycle("ABC", null, null)).build()
  );
  private final static Map<AccountKey, Iterable<BillingAccount>> ACCOUNTS_MAP = Stream.ofAll(BILLING_ACCOUNTS)
      .groupBy(a -> new AccountKey(a.getSubAccountId(), DATE))
      .mapValues(Stream::asJava);

  public static final BillingCycle WPMO = new BillingCycle("WPMO", LocalDate.of(2021, 1, 1), LocalDate.of(2021, 1, 31));
  private final static List<BillingCycle> BILLING_CYCLES = List.of(
      WPMO);

  private final static List<AccountDetails> ACCOUNT_DETAILS = List.of(
      new AccountDetails("ac1", "ROW", "WPMO"),
      new AccountDetails("ac1", null, "null"),
      new AccountDetails("ac2", "ROW", "WPMO"),
      new AccountDetails("ac4", null, "null")
  );
  private final static Map<String, Iterable<AccountDetails>> ACCOUNTS_DETAILS_MAP = Stream.ofAll(ACCOUNT_DETAILS)
      .groupBy(AccountDetails::getAccountId)
      .mapValues(Stream::asJava);

  private final static Map<String, Iterable<BillingAccount>> PARTY_ID_ACCOUNTS_MAP = List.of(
      BILLING_ACCOUNT.get().partyId("P0007")
          .legalCounterparty("P0001")
          .currency("GBP")
          .accountType("CHRG")
          .subAccountType("CHRG")
          .subAccountId("0007")
      .build()).groupBy(BillingAccount::getPartyId)
      .mapValues(List::asJava);

  private final static DefaultAccountDeterminationService service = buildservice();

  @Test
  @DisplayName("When validate accounts with null list then return validation error")
  void whenValidateAccountWithNullValueThenReturnError() {
    assertInvalid(AccountDeterminationService.validateAccounts(new AccountKey("1",DATE).toString(), (Iterable<BillingAccount> ) null),
        "Could not determine account details for key AccountDeterminationService.AccountKey(subAccountId=1, accruedDate=2021-01-10)"
    );
  }

  @Test
  @DisplayName("When validate accounts with empty list then return validation error")
  void whenValidateAccountsWithEmptyListThenReturnError() {
    assertInvalid(AccountDeterminationService.validateAccounts(new AccountKey("1", DATE).toString(),
        List.empty()),
        "Could not determine account details for key AccountDeterminationService.AccountKey(subAccountId=1, accruedDate=2021-01-10)"
    );
  }

  @Test
  @DisplayName("When validate account details with null list then return validation error")
  void whenValidateAccountDetailsWithNullValueThenReturnError() {
    assertInvalid(AccountDeterminationService.validateAccountDetails("1", (Iterable<AccountDetails> ) null),
        "Could not determine processing group and billing cycle code for account id 1"
    );
  }

  @Test
  @DisplayName("When no account is found then return validation error")
  void whenNoAccountFoundThenDeterminationFails() {
    assertInvalid(service.findAccount(new AccountKey("3", DATE)),
        "Could not determine account details for key AccountDeterminationService.AccountKey(subAccountId=3, accruedDate=2021-01-10)"
    );
  }

  @Test
  @DisplayName("When two account entries are found then return validation error")
  void whenMultipleAccountsFoundThenDeterminationFails() {
    assertInvalid(service.findAccount(new AccountKey("2", DATE)),
        "Found multiple entries for account details for key AccountDeterminationService.AccountKey(subAccountId=2, accruedDate=2021-01-10)"
    );
  }

  @Test
  @DisplayName("When the account has no billing cycle then return validation error")
  void whenAccountWithEmptyBillCycleThenDeterminationFails() {
    assertInvalid(service.findAccount(new AccountKey("4", DATE)),
        "No bill cycle found for code ABC and date 2021-01-10"
    );
  }

  @Test
  @DisplayName("When only one account is found then return correct value")
  void whenOnlyOneAccountIsFoundThenReturnCorrectValue() {
    Validation<DomainError, BillingAccount> result = service.findAccount(new AccountKey("1", DATE));
    BillingAccount expected = BILLING_ACCOUNT.get().subAccountId("1").build();

    assertThat(result.isValid(), is(true));
    assertThat(result.get(), is(expected));
  }

  @Test
  @DisplayName("When select processing group then return correct value")
  void whenGetProcessingGroupForAccountIdReturnCorrectValue() {
    Validation<DomainError, String> result = service.getProcessingGroupForAccountId("ac4");
    assertThat(result.get(), is(nullValue()));
  }

  @Test
  @DisplayName("When select billing cycle code then return correct value")
  void whenGetBillingCycleCodeForAccountIdReturnCorrectValue() {
    Validation<DomainError, String> result = service.getBillingCycleCodeForAccountId("ac2");
    assertThat(result.get(), is("WPMO"));
  }

  @Test
  @DisplayName("When select valid billing cycle for account id then return correct value")
  void whenSelectValidBillingCycleThenReturnCorrectValue() {
    Validation<DomainError, BillingCycle> result = service.getBillingCycleForAccountId("ac2",
        LocalDate.of(2021, 1, 10));

    assertThat(result.get(),is(WPMO));
  }

  @Test
  @DisplayName("When select billing cycle for account id with non existent code then return error")
  void whenSelectInvalidBillingCycleThenReturnDomainError() {
    Validation<DomainError, BillingCycle> result = service.getBillingCycleForAccountId("ac4",
        LocalDate.of(2021, 1, 10));

    assertInvalid(result, "No bill cycle found for code null and date 2021-01-10");
  }

  @Test
  @DisplayName("When no account details records found for account id then return domain error")
  void whenNoDataFoundReturnValidationError() {
    assertInvalid(service.getProcessingGroupForAccountId("ac3"),
        "Could not determine processing group and billing cycle code for account id AC3"
    );
  }

  @Test
  @DisplayName("When multiple account details records found for account id then return domain error")
  void whenMultipleRecordsFoundReturnValidationError() {
    assertInvalid(service.getProcessingGroupForAccountId("ac1"),
        "Multiple rows found when trying to get processing group and billing cycle code for account id AC1"
    );
  }

  @Test
  @DisplayName("When get charging account by party id and legal counterparty then return correct value")
  void whenGetAccountForPartyIdAndLCPReturnCorrectValue() {
    Validation<DomainError, BillingAccount> result = service.getChargingAccountByPartyId("P0007", "P0001", "GBP");
    BillingAccount expected = BILLING_ACCOUNT.get()
        .partyId("P0007")
        .legalCounterparty("P0001")
        .currency("GBP")
        .accountType("CHRG")
        .subAccountType("CHRG")
        .subAccountId("0007")
        .build();

    assertThat(result.isValid(), is(true));
    assertThat(result.get(), is(expected));
  }

  @Test
  @DisplayName("When no account details records found for account id then return domain error")
  void whenGetByPartyIdAndNoDataFoundReturnValidationError() {
    assertInvalid(service.getChargingAccountByPartyId("XXX", "XXX", "GBP"),
        "Could not determine account details for key partyId=XXX, legalCounterpartyId=XXX, currency=GBP, accountType=CHRG"
    );
  }

  private static <T> void assertInvalid(Validation<DomainError, T> result, String... error) {
    assertThat(result.isInvalid(), is(true));
    for (String s : error) {
      assertThat(result.getError().getMessage(), is(s));
    }
  }

  static DefaultAccountDeterminationService buildservice() {

    AccountRepository accountRepository = new AccountRepository() {
      @Override
      public Iterable<BillingAccount> getAccountBySubAccountId(AccountKey key) {
        return ACCOUNTS_MAP.get(key).getOrElse(Collections.emptyList());
      }

      @Override
      public Iterable<BillingCycle> getBillingCycles() {
        return BILLING_CYCLES;
      }

      @Override
      public Iterable<AccountDetails> getAccountDetailsByAccountId(String accountId) {
        return ACCOUNTS_DETAILS_MAP.get(accountId).getOrElse(Collections.emptyList());
      }

      @Override
      public Iterable<Party> getParties() {
        return null;
      }

      @Override
      public Iterable<BillingAccount> getChargingAccountByPartyId(String partyId, String legalCounterParty, String currency) {
        return PARTY_ID_ACCOUNTS_MAP.get(partyId).getOrElse(Collections.emptyList());
      }
    };

    return new DefaultAccountDeterminationService(2, 2, 10, accountRepository);
  }
}