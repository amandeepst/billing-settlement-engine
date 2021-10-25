package com.worldpay.pms.pba.engine.data;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static java.lang.String.format;

import com.worldpay.pms.pba.domain.model.WithholdFunds;
import com.worldpay.pms.pba.domain.model.WithholdFundsBalance;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.repositories.Sql2oRepository;
import io.vavr.Tuple2;
import java.sql.Date;
import java.time.LocalDate;
import java.util.function.Function;
import org.sql2o.Sql2oQuery;

public class DefaultPostBillingRepository extends Sql2oRepository implements PostBillingRepository {

  private final LocalDate logicalDate;

  public DefaultPostBillingRepository(JdbcConfiguration conf, LocalDate logicalDate) {
    super(conf);
    this.logicalDate = logicalDate;
  }

  @Override
  public Iterable<WithholdFunds> getWithholdFunds() {
    String name = "get-withhold-funds";
    return db.execQuery(
        name, resourceAsString(format("sql/input/%s.sql", name)),
        q -> q
            .addParameter("logical_date", Date.valueOf(logicalDate))
            .executeAndFetch(WithholdFunds.class)
    );
  }

  @Override
  public Iterable<Tuple2<String, String>> getWafSubAccountIds() {
    return exec("get-waf-sub-account-ids", "",
        q -> q
            .addParameter("logicalDate", Date.valueOf(logicalDate))
            .executeAndFetch(handler(rs -> new Tuple2<>(
                rs.getAndTrimString("accountId"),
                rs.getAndTrimString("subAccountId"))))
    );
  }

  @Override
  public Iterable<WithholdFundsBalance> getWithholdFundsBalance(String partyId, String legalCounterparty, String currency) {
    return exec("get-withhold-funds-balance-by-party", "",
        q -> q
            .addParameter("per_id_nbr", partyId)
            .addParameter("cis_division", legalCounterparty)
            .addParameter("currency_cd", currency)
            .executeAndFetch(WithholdFundsBalance.class)
    );
  }

  private <T> T exec(String name, String hints, Function<Sql2oQuery, T> expression) {
    return db.execQuery(
        name,
        resourceAsString(format("sql/input/%s.sql", name)).replace(":hints", hints),
        expression
    );
  }
}
