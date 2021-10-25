package com.worldpay.pms.bse.engine.data;

import static com.worldpay.pms.bse.engine.utils.DatabaseCsvUtils.checkThatDataFromCsvFileIsTheSameAsThatInList;
import static com.worldpay.pms.bse.engine.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountDetails;
import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountKey;
import com.worldpay.pms.bse.domain.account.BillingAccount;
import com.worldpay.pms.bse.domain.account.BillingCycle;
import com.worldpay.pms.bse.domain.model.tax.Party;
import com.worldpay.pms.bse.engine.BillingConfiguration;
import com.worldpay.pms.bse.engine.utils.WithDatabaseAndSpark;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.junit.SparkContext;
import com.worldpay.pms.testing.utils.DbUtils;
import java.time.LocalDate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.sql2o.Sql2oQuery;

class JdbcAccountRepositoryTest implements WithDatabaseAndSpark {

  private SqlDb mduDb;
  private SqlDb cisadmDb;
  private SqlDb billingDb;
  private JdbcAccountRepository repository;

  @Override
  public void bindMduJdbcConfiguration(JdbcConfiguration conf) {
    this.mduDb = SqlDb.simple(conf);
  }

  @Override
  public void bindCisadmJdbcConfiguration(JdbcConfiguration conf) {
    this.cisadmDb = SqlDb.simple(conf);
  }

  @Override
  public void bindBillingConfiguration(BillingConfiguration conf) {
    this.repository = new JdbcAccountRepository(conf.getSources().getAccountData(), LocalDate.of(2021, 1, 1));
  }

  @Override
  public void bindBillingJdbcConfiguration(JdbcConfiguration conf) {
    this.billingDb = SqlDb.simple(conf);
  }

  @AfterAll
  static void afterAll() {
    SparkContext.reset();
  }

  @BeforeEach
  void setUp() {
    cleanupDatabase();
    populateDatabase();
  }

  @Test
  @DisplayName("When no account data is found then return empty iterable")
  void whenNoDataThenReturnEmptyIterable() {
    Iterable<BillingAccount> accounts = repository
        .getAccountBySubAccountId(new AccountKey("test", LocalDate.of(2021, 1, 18)));
    assertThat(accounts.iterator().hasNext(), is(equalTo(false)));
  }

  @Test
  @DisplayName("When account has expired acct hierarchy then return correct data")
  void whenAccountHasNoParentThenReturnCorrectData() {
    Iterable<BillingAccount> accounts = repository.getAccountBySubAccountId(
        new AccountKey("sa1", LocalDate.of(2021, 1, 21)));

    checkThatDataFromCsvFileIsTheSameAsThatInList(
        "input/account-data/expected_account_no_parent.csv",
        StreamSupport.stream(accounts.spliterator(), false).collect(Collectors.toList()),
        BillingAccount.class);

  }

  @Test
  @DisplayName("When account has parent return the correct data")
  void whenAccountHasParentThenReturnCorrectData() {
    Iterable<BillingAccount> accounts = repository.getAccountBySubAccountId(
        new AccountKey("sa2", LocalDate.of(2021, 1, 21)));

    checkThatDataFromCsvFileIsTheSameAsThatInList("input/account-data/expected_acount_parent.csv",
        StreamSupport.stream(accounts.spliterator(), false).collect(Collectors.toList()),
        BillingAccount.class);
  }

  @Test
  @DisplayName("When sub account or account is inactive then return no data")
  void whensubAccountIsInactiveThenReturnNoData() {
    Iterable<BillingAccount> accounts = repository.getAccountBySubAccountId(
        new AccountKey("sa3", LocalDate.of(2021, 1, 21)));

    assertThat(accounts.iterator().hasNext(), is(equalTo(false)));

    accounts = repository.getAccountBySubAccountId(
        new AccountKey("sa4", LocalDate.of(2021, 1, 21)));

    assertThat(accounts.iterator().hasNext(), is(equalTo(false)));
  }

  @Test
  @DisplayName("When sub account or accout or party is invalid then return no data")
  void whensubAccountIsInvalidThenReturnNoData() {
    Iterable<BillingAccount> accounts = repository.getAccountBySubAccountId(
        new AccountKey("sa5", LocalDate.of(2021, 1, 21)));

    assertThat(accounts.iterator().hasNext(), is(equalTo(false)));

    accounts = repository.getAccountBySubAccountId(
        new AccountKey("sa6", LocalDate.of(2021, 1, 21)));

    assertThat(accounts.iterator().hasNext(), is(equalTo(false)));
  }

  @Test
  @DisplayName("When no bill cycle data is found then return empty iterable")
  void whenNoBillCycleDataThenReturnEmptyIterable() {
    cleanupDatabase();
    Iterable<BillingCycle> billingCycles = repository.getBillingCycles();
    assertThat(billingCycles.iterator().hasNext(), is(equalTo(false)));
  }

  @Test
  @DisplayName("When billing cycles found then return the correct data and ignore those that are more that 1 year from the logical date. ")
  void whenBillingCyclesFoundThenReturnCorrectData() {
    Iterable<BillingCycle> billingCycles = repository.getBillingCycles();

    checkThatDataFromCsvFileIsTheSameAsThatInList("input/account-data/expected_billing_cycle.csv",
        StreamSupport.stream(billingCycles.spliterator(), false).collect(Collectors.toList()),
        BillingCycle.class);
  }

  @Test
  @DisplayName("")
  void whenGetAccountDetailsByAccountIdAndNoDataFoundReturnEmpty() {
    Iterable<AccountDetails> accountDetails = repository.getAccountDetailsByAccountId("ac1");

    checkThatDataFromCsvFileIsTheSameAsThatInList("input/account-data/expected_account_details_by_account_id.csv",
        StreamSupport.stream(accountDetails.spliterator(), false).collect(Collectors.toList()),
        AccountDetails.class);
  }

  @Test
  @DisplayName("When get account details by account id and no data found then return empty list")
  void whenGetProcessingGroupAndBillingCycleIdThenReturnCorrectData() {
    Iterable<AccountDetails> accountDetails = repository.getAccountDetailsByAccountId("ac9");

    assertThat(accountDetails.iterator().hasNext(), is(equalTo(false)));
  }

  @Test
  @DisplayName("When party data then return correct list")
  void whenGetPartyThenReturnCorrectData() {
    Iterable<Party> parties = repository.getParties();

    checkThatDataFromCsvFileIsTheSameAsThatInList("input/account-data/expected_party.csv",
        StreamSupport.stream(parties.spliterator(), false).collect(Collectors.toList()),
        Party.class);
  }

  @Test
  @DisplayName("When get charging account data by party id for the parent party then return correct result")
  void whenGetAccountByPartyIdAndNoParentPartyThenReturnCorrectResult() {
    Iterable<BillingAccount> accounts = repository.getChargingAccountByPartyId("P0008", "P0001", "GBP");
    checkThatDataFromCsvFileIsTheSameAsThatInList(
        "input/account-data/expected_account_by_party_id_no_parent.csv",
        StreamSupport.stream(accounts.spliterator(), false).collect(Collectors.toList()),
        BillingAccount.class);
  }

  private void cleanupDatabase() {
    DbUtils.cleanUpWithoutMetadata(mduDb, "batch_history", "acct", "acct_hier", "party", "sub_acct");
    DbUtils.cleanUpWithoutMetadata(cisadmDb, "ci_bill_cyc_sch");
  }

  private void populateDatabase() {
    readFromCsvFileAndWriteToExistingTable(mduDb, "input/account-data/batch_history.csv",
        "BATCH_HISTORY");
    readFromCsvFileAndWriteToExistingTable(mduDb, "input/account-data/acct.csv", "ACCT");
    readFromCsvFileAndWriteToExistingTable(mduDb, "input/account-data/acct_hier.csv", "ACCT_HIER");
    readFromCsvFileAndWriteToExistingTable(mduDb, "input/account-data/party.csv", "PARTY");
    readFromCsvFileAndWriteToExistingTable(mduDb, "input/account-data/sub_acct.csv", "SUB_ACCT");
    readFromCsvFileAndWriteToExistingTable(cisadmDb, "input/account-data/ci_bill_cyc_sch.csv",
        "CI_BILL_CYC_SCH");

    billingDb.execQuery("refresh-acct-data-materialized-view",
        "{call pkg_billing_engine.prc_refresh_account()}",
        Sql2oQuery::executeUpdate);
  }

}