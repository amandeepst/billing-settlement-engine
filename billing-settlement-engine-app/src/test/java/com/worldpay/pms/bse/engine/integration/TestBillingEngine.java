package com.worldpay.pms.bse.engine.integration;

import static com.typesafe.config.ConfigFactory.load;
import static java.lang.String.valueOf;

import com.typesafe.config.Config;
import com.worldpay.pms.bse.engine.BillingConfiguration;
import com.worldpay.pms.bse.engine.BillingEngine;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import java.sql.Date;

public class TestBillingEngine extends BillingEngine {

  private static JdbcConfiguration inputDbConfig;

  public TestBillingEngine(Config conf) {
    super(load("application-test.conf"));
  }

  public static void submit(JdbcConfiguration inputDbConfig, Date date, String processingGroup, String billingType,
      boolean overrideEventDate) {

    TestBillingEngine.inputDbConfig = inputDbConfig;
    run(TestBillingEngine::new, "submit", "--force",
        "--logical-date", valueOf(date),
        "--processing-group", processingGroup,
        "--billing-type", billingType,
        overrideEventDate ? "--override-event-date" : "");
  }

  @Override
  protected BillingConfiguration getSettingsConfig(Config config, Class<BillingConfiguration> clazz) {
    BillingConfiguration  settings = super.getSettingsConfig(config, clazz);
    settings.getFailedBillsThreshold().setFailedBillRowsThresholdPercent(25);
    settings.getSources().getBillableItems().setPartitionCount(12);
    settings.getSources().getBillableItems().setPrintExecutionPlan(false);
    settings.getSources().getBillableItems().getDataSource().setUrl(inputDbConfig.getUrl());
    settings.getSources().getBillableItems().getDataSource().setUser(inputDbConfig.getUser());
    settings.getSources().getBillableItems().getDataSource().setPassword(inputDbConfig.getPassword());
    settings.getSources().getBillableItems().getDataSource().setDriver(inputDbConfig.getDriver());
    settings.getSources().getMiscBillableItems().setPartitionCount(12);
    settings.getSources().getMiscBillableItems().setPrintExecutionPlan(false);
    settings.getSources().getMiscBillableItems().getDataSource().setUrl(inputDbConfig.getUrl());
    settings.getSources().getMiscBillableItems().getDataSource().setUser(inputDbConfig.getUser());
    settings.getSources().getMiscBillableItems().getDataSource().setPassword(inputDbConfig.getPassword());
    settings.getSources().getMiscBillableItems().getDataSource().setDriver(inputDbConfig.getDriver());
    settings.getSources().getFailedBillableItems().setPartitionCount(12);
    settings.getSources().getFailedBillableItems().setPrintExecutionPlan(false);
    settings.getSources().getFailedBillableItems().getDataSource().setUrl(inputDbConfig.getUrl());
    settings.getSources().getFailedBillableItems().getDataSource().setUser(inputDbConfig.getUser());
    settings.getSources().getFailedBillableItems().getDataSource().setPassword(inputDbConfig.getPassword());
    settings.getSources().getFailedBillableItems().getDataSource().setDriver(inputDbConfig.getDriver());
    settings.getSources().getAccountData().getConf().setUrl(inputDbConfig.getUrl());
    settings.getSources().getAccountData().getConf().setUser(inputDbConfig.getUser());
    settings.getSources().getAccountData().getConf().setPassword(inputDbConfig.getPassword());
    settings.getSources().getAccountData().getConf().setDriver(inputDbConfig.getDriver());
    settings.getSources().getStaticData().getConf().setUrl(inputDbConfig.getUrl());
    settings.getSources().getStaticData().getConf().setUser(inputDbConfig.getUser());
    settings.getSources().getStaticData().getConf().setPassword(inputDbConfig.getPassword());
    settings.getSources().getStaticData().getConf().setDriver(inputDbConfig.getDriver());

    settings.getSources().getMinCharge().getDataSource().setUrl(inputDbConfig.getUrl());
    settings.getSources().getMinCharge().getDataSource().setUser(inputDbConfig.getUser());
    settings.getSources().getMinCharge().getDataSource().setPassword(inputDbConfig.getPassword());
    settings.getSources().getMinCharge().getDataSource().setDriver(inputDbConfig.getDriver());
    return settings;
  }
}
