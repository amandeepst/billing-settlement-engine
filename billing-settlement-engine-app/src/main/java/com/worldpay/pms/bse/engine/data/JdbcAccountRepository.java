package com.worldpay.pms.bse.engine.data;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static java.lang.String.format;

import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountDetails;
import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountKey;
import com.worldpay.pms.bse.domain.account.AccountRepository;
import com.worldpay.pms.bse.domain.account.BillingAccount;
import com.worldpay.pms.bse.domain.account.BillingCycle;
import com.worldpay.pms.bse.domain.model.tax.Party;
import com.worldpay.pms.bse.engine.BillingConfiguration.AccountRepositoryConfiguration;
import com.worldpay.pms.spark.core.SparkUtils;
import com.worldpay.pms.spark.core.SparkUtils.LoggingStrategy;
import com.worldpay.pms.spark.core.TryFunction;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.spark.core.jdbc.repositories.Sql2oRepository;
import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Collections;
import java.util.function.Function;
import java.util.function.Supplier;
import org.sql2o.ResultSetHandler;
import org.sql2o.Sql2oQuery;

public class JdbcAccountRepository extends Sql2oRepository implements AccountRepository {

  private static final long THRESHOLD_FOR_LOGGING_IN_MS = 1000;
  private static final LoggingStrategy LOGGING_STRATEGY = new LoggingStrategy() {
    @Override
    public <T> T timed(String description, Supplier<T> supplier) {
      return SparkUtils.timed(description, THRESHOLD_FOR_LOGGING_IN_MS, supplier);
    }
  };

  private AccountRepositoryConfiguration conf;
  private LocalDate logicalDate;

  public JdbcAccountRepository(AccountRepositoryConfiguration conf, LocalDate logicalDate) {
    super(SqlDb.pooled(conf.getConf(), Collections.emptyMap()).withLoggingStrategy(LOGGING_STRATEGY));
    this.conf = conf;
    this.logicalDate = logicalDate;
  }

  @Override
  public Iterable<BillingAccount> getAccountBySubAccountId(AccountKey key) {
    return exec("get-account", conf.getAccountDetailsHints(), q ->
        q.addParameter("subAcctId", key.getSubAccountId())
            .addParameter("logicalDate", Date.valueOf(key.getAccruedDate()))
            .executeAndFetch(handler(getBillingAccountMapper()))
    );
  }

  @Override
  public Iterable<BillingCycle> getBillingCycles() {
    return exec("get-billing-cycle", "", q ->
        q.addParameter("minDate", Date.valueOf(logicalDate.minusYears(1)))
            .addParameter("maxDate", Date.valueOf(logicalDate.plusYears(1)))
            .executeAndFetch(handler(rs -> new BillingCycle(
                ObjectPool.intern(rs.getAndTrimString("billCycleCode")),
                rs.getDate("winStartDate").toLocalDate(),
                rs.getDate("winEndDate").toLocalDate())))
    );
  }

  @Override
  public Iterable<AccountDetails> getAccountDetailsByAccountId(String accountId) {
    return exec("get-account-by-acctid", "", q ->
        q
            .addParameter("accountId", accountId)
            .executeAndFetch(handler(rs -> new AccountDetails(
                accountId,
                ObjectPool.intern(rs.getAndTrimString("processingGroup")),
                ObjectPool.intern(rs.getAndTrimString("billCycleCode")))))
    );
  }

  public Iterable<Party> getParties() {
    return exec("get-party-data", "",
        q -> q.addParameter("logicalDate", Date.valueOf(logicalDate))
            .executeAndFetch((ResultSetHandler<Party>) resultSet -> {
              String merchantTaxRegistration = ObjectPool.intern(resultSet.getString("merchantTaxRegistration"));
              return new Party(
                  ObjectPool.intern(resultSet.getString("partyId")),
                  ObjectPool.intern(resultSet.getString("countryId")),
                  null,
                  ObjectPool.intern(merchantTaxRegistration), null
              );
            })
    );
  }

  @Override
  public Iterable<BillingAccount> getChargingAccountByPartyId(String partyId, String legalCounterParty, String currency) {
    return exec("get-account-by-party-id", conf.getAccountDetailsHints(), q ->
        q.addParameter("partyId", partyId)
            .addParameter("lcp", legalCounterParty)
            .addParameter("currency", currency)
            .addParameter("logicalDate", Date.valueOf(logicalDate))
            .executeAndFetch(handler(getBillingAccountMapper()))
    );
  }

  private <T> T exec(String name, String hints, Function<Sql2oQuery, T> expression) {
    return db.execQuery(
        name,
        resourceAsString(format("sql/static-data/%s.sql", name)).replace(":hints", hints),
        expression
    );
  }

  private TryFunction<ResultSetWrapper, BillingAccount, SQLException> getBillingAccountMapper() {
    return rs ->
        BillingAccount.builder()
            .accountId(ObjectPool.intern(rs.getAndTrimString("accountId")))
            .childPartyId(ObjectPool.intern(rs.getAndTrimString("childPartyId")))
            .accountType(ObjectPool.intern(rs.getAndTrimString("accountType")))
            .subAccountId(ObjectPool.intern(rs.getAndTrimString("parentSubAccountId")))
            .subAccountType(ObjectPool.intern(rs.getAndTrimString("subAccountType")))
            .partyId(ObjectPool.intern(rs.getAndTrimString("partyId")))
            .legalCounterparty(ObjectPool.intern(rs.getAndTrimString("legalCounterparty")))
            .currency(ObjectPool.intern(rs.getAndTrimString("currencyCode")))
            .businessUnit(ObjectPool.intern(rs.getAndTrimString("businessUnit")))
            .processingGroup(ObjectPool.intern(rs.getAndTrimString("processingGroup")))
            .billingCycle(new BillingCycle(ObjectPool.intern(rs.getAndTrimString("billingCycleCode")), null, null))
            .build();

  }
}
