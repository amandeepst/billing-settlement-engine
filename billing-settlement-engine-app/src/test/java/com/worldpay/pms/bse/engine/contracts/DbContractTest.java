package com.worldpay.pms.bse.engine.contracts;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.worldpay.pms.bse.engine.BillingConfiguration;
import com.worldpay.pms.bse.engine.utils.WithDatabase;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import java.io.Serializable;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.sql2o.data.Table;

@Slf4j
public abstract class DbContractTest implements WithDatabase {

  protected SqlDb db;

  @Override
  public void bindBillingConfiguration(BillingConfiguration conf) {
    db = SqlDb.simple(conf.getSources().getPendingConfiguration().getPendingBill().getDataSource());
  }

  String runAndGetPlan(String name, String sql, Map<String, Serializable> bindArguments) {
    Table result = db.execQuery(name, sql, q -> q.withParams(bindArguments).executeAndFetchTable());
    assertThat(result, is(notNullValue()));

    // get plan
    String explainPlanStatement = String.format("EXPLAIN PLAN SET STATEMENT_ID = '%s' FOR %s", StringUtils.left(name, 30), sql);
    String fetchExplainPlan = String.format("SELECT * FROM TABLE(dbms_xplan.display('PLAN_TABLE', '%s', 'TYPICAL'))", StringUtils.left(name, 30));
    return db.execQuery("output-execution-plan", explainPlanStatement, query ->
        String.join("\n", query.withParams(bindArguments)
            .executeUpdate()
            .createQuery(fetchExplainPlan)
            .executeAndFetch(String.class))
    );
  }
}
