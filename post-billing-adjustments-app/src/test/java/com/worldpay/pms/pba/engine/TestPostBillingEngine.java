package com.worldpay.pms.pba.engine;

import static com.typesafe.config.ConfigFactory.load;
import static java.lang.String.valueOf;

import com.typesafe.config.Config;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import java.sql.Date;

public class TestPostBillingEngine extends PostBillingEngine {

  private static JdbcConfiguration inputDbConfig;

  public TestPostBillingEngine(Config conf) {
    super(load("application-test.conf"));
  }

  public static void submit(Date date, JdbcConfiguration inputDbConfig) {

    TestPostBillingEngine.inputDbConfig = inputDbConfig;
    run(TestPostBillingEngine::new, "submit", "--force",
        "--logical-date", valueOf(date));
  }

  @Override
  protected PostBillingConfiguration getSettingsConfig(Config config, Class<PostBillingConfiguration> clazz) {
    PostBillingConfiguration settings = super.getSettingsConfig(config, clazz);
    settings.getSources().getWithholdFunds().setPartitionCount(12);
    settings.getSources().getWithholdFunds().setPrintExecutionPlan(false);
    settings.getSources().getWithholdFunds().getDataSource().setUrl(inputDbConfig.getUrl());
    settings.getSources().getWithholdFunds().getDataSource().setUser(inputDbConfig.getUser());
    settings.getSources().getWithholdFunds().getDataSource().setPassword(inputDbConfig.getPassword());
    settings.getSources().getWithholdFunds().getDataSource().setDriver(inputDbConfig.getDriver());
    return settings;
  }
}
