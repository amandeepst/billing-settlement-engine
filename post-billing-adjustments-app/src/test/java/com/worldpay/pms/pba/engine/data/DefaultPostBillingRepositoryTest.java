package com.worldpay.pms.pba.engine.data;

import static com.worldpay.pms.testing.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.worldpay.pms.pba.engine.PostBillingConfiguration;
import com.worldpay.pms.pba.domain.model.WithholdFunds;
import com.worldpay.pms.pba.domain.model.WithholdFundsBalance;
import com.worldpay.pms.pba.engine.utils.WithDatabase;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.utils.DbUtils;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import io.vavr.collection.Traversable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sql2o.Sql2oQuery;

class DefaultPostBillingRepositoryTest implements WithDatabase {

  private SqlDb mduDb;
  private SqlDb mamDb;
  private JdbcConfiguration wafSourceConfiguration;
  private DefaultPostBillingRepository postBillingRepository;

  @Override
  public void bindPostBillingConfiguration(PostBillingConfiguration conf) {
    this.wafSourceConfiguration = conf.getSources().getWithholdFunds().getDataSource();
  }

  @Override
  public void bindMduJdbcConfiguration(JdbcConfiguration conf) {
    this.mduDb = SqlDb.simple(conf);
  }

  @Override
  public void bindMamJdbcConfiguration(JdbcConfiguration conf) {
    this.mamDb = SqlDb.simple(conf);
  }

  @BeforeEach
  void cleanup() {
    DbUtils.cleanUpWithoutMetadata(mduDb, "batch_history", "withhold_funds", "sub_acct");
    DbUtils.cleanUpWithoutMetadata(mamDb, "cm_merch_acct_batch_history", "cm_merch_acct_ledger_balance");

    postBillingRepository = new DefaultPostBillingRepository(wafSourceConfiguration, LocalDate.parse("2020-01-01"));
  }

  @Test
  void whenAccountIdMatchesAndLogicalDateIsValidThenWithholdFundsEntryIsGenerated() {
    readFromCsvFileAndWriteToExistingTable(mduDb, "input/withhold-funds/batch_history.csv", "batch_history");
    readFromCsvFileAndWriteToExistingTable(mduDb, "input/withhold-funds/withhold-funds.csv", "withhold_funds");

    Iterable<WithholdFunds> withholdFunds = postBillingRepository.getWithholdFunds();
    Map<String, WithholdFunds> map = Stream.ofAll(withholdFunds)
        .groupBy(WithholdFunds::getAccountId)
        .mapValues(Traversable::get)
        .toJavaMap();

    assertThat(map.get("id_1").getWithholdFundPercentage(), is(99.99));
    assertNull(map.get("id_2"));
    assertNull(map.get("id_3"));
  }

  @Test
  void whenGetWafSubAccountIdThenReturnCorrectValue() {
    readFromCsvFileAndWriteToExistingTable(mduDb, "input/withhold-funds/batch_history.csv", "batch_history");
    readFromCsvFileAndWriteToExistingTable(mduDb, "input/sub-account/sub_acct.csv", "sub_acct");
    readFromCsvFileAndWriteToExistingTable(mduDb, "input/sub-account/withhold-funds.csv", "withhold_funds");

    DefaultPostBillingRepository repository = new DefaultPostBillingRepository(wafSourceConfiguration,
        LocalDate.of(2022, 2, 20));

    Iterable<Tuple2<String, String>> wafSubAccounts = repository.getWafSubAccountIds();

    Map<String, String> subAccountsMap = Stream.ofAll(wafSubAccounts).toJavaMap(Function.identity());
    assertThat(subAccountsMap.size(), is(1));
    assertThat(subAccountsMap.get("ac1"), is("sa1"));
    assertNull(subAccountsMap.get("ac2"));
  }

  @Test
  void whenGetWithholdFundsBalanceForPartyThenReturnCorrectBalance() {
    readFromCsvFileAndWriteToExistingTable(mamDb, "input/merch-acct-ledger-balance/cm_merch_acct_batch_history.csv", "cm_merch_acct_batch_history");
    readFromCsvFileAndWriteToExistingTable(mamDb, "input/merch-acct-ledger-balance/cm_merch_acct_ledger_balance.csv", "cm_merch_acct_ledger_balance");

    mamDb.execQuery("refresh-merch-acct-ledger-snapshot-materialized-view",
        "{call CBE_MAM_OWNER.PKG_MERCHANT_ACCOUNTING.prc_refresh_ledger_snapshot()}",
        Sql2oQuery::executeUpdate);

    List<WithholdFundsBalance> withholdFundsBalance =
        List.ofAll(postBillingRepository.getWithholdFundsBalance("PO4004921989", "00001", "GBP"));
    assertThat(withholdFundsBalance.size(), is(1));
    assertThat(withholdFundsBalance.get(0),
        equalTo(new WithholdFundsBalance("PO4004921989", "00001", "GBP", BigDecimal.valueOf(-13110))));
  }
}