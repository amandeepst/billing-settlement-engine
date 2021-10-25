package com.worldpay.pms.pba.engine;

import static com.worldpay.pms.spark.core.SparkApp.run;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.beust.jcommander.ParameterException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.worldpay.pms.cli.CommandLineApp;
import com.worldpay.pms.cli.PmsParameterDelegate;
import com.worldpay.pms.pba.engine.utils.WithDatabase;
import com.worldpay.pms.spark.core.PMSException;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.utils.DbUtils;
import java.util.function.Function;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

public class PostBillingEngineTest implements WithDatabase {

  static SqlDb sqlDb;

  public void bindPostBillingJdbcConfiguration(JdbcConfiguration conf) {
    sqlDb = SqlDb.simple(conf);
  }

  @Test
  void testLogicalDateAcceptsCtrlMFormat() {
    PostBillingEngine postBillingEngine = new PostBillingEngine(ConfigFactory.load("application-test.conf"));

    Function<Config, CommandLineApp> app = cfg -> postBillingEngine;

    run(app, "submit", "--force", "--logical-date", "16/06/2020");
    assertEquals("The logical date provided as param should be in app.", "2020-06-16", postBillingEngine.logicalDate.toString());

    run(app, "submit", "--force", "--logical-date", "30/09/2020");
    assertEquals("The logical date provided as param should be in app.", "2020-09-30", postBillingEngine.logicalDate.toString());
  }


  @Test
  void testFailWhenLogicalDateParamIsNotPresent() {
    assertThrows(ParameterException.class, () -> PostBillingEngine.main("submit", "--force"));
  }


  @Test
  void testFailWhenParamDelegate() {
    PostBillingEngine postBillingEngine = new PostBillingEngine(ConfigFactory.load("application-test.conf")) {
      @Override
      public PmsParameterDelegate getParameterDelegate() {
        return new PmsParameterDelegate() {
        };
      }
    };
    Function<Config, CommandLineApp> app = cfg -> postBillingEngine;

    assertThrows(PMSException.class, () -> run(app, "submit", "--force"));
  }

  @AfterAll
  static void cleanUp(){
    DbUtils.cleanUp(sqlDb, "batch_history_post_billing", "outputs_registry_post_billing");
  }

}