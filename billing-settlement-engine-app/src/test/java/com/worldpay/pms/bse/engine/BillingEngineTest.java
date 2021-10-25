package com.worldpay.pms.bse.engine;

import static com.worldpay.pms.spark.core.SparkApp.run;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.beust.jcommander.ParameterException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.worldpay.pms.bse.domain.exception.BillingException;
import com.worldpay.pms.bse.engine.BillingConfiguration.PublisherConfiguration;
import com.worldpay.pms.bse.engine.data.BillingBatchHistoryRepository;
import com.worldpay.pms.bse.engine.utils.WithDatabase;
import com.worldpay.pms.cli.CommandLineApp;
import com.worldpay.pms.cli.PmsParameterDelegate;
import com.worldpay.pms.spark.core.batch.Batch;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.utils.DbUtils;
import java.time.LocalDate;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BillingEngineTest implements WithDatabase {

  private SqlDb db;

  @Override
  public void bindBillingJdbcConfiguration(JdbcConfiguration conf) {
    db = SqlDb.simple(conf);
  }

  @BeforeEach
  void setUp() {
    DbUtils.cleanUp(db);
  }

  @Test
  void testRunIdCanBeOverriddenFromCommandLine() {
    BillingEngine billingEngine = new BillingEngine(ConfigFactory.load("application-test.conf")) {
      @Override
      protected BillingBatchHistoryRepository buildBatchHistory() {
        return new BillingBatchHistoryRepository(getConf().getDb(), LocalDate.of(2020, 6, 16), new PublisherConfiguration()) {
          @Override
          public long generateRunId(BatchId batchId) {
            return 100L;
          }
        };
      }

      @Override
      public Batch run(Batch batch) {
        assertThat(generateRunId.apply(batch.id), is(15L));
        assertThat(logicalDate, is(LocalDate.parse("2020-06-16")));
        return batch;
      }
    };

    run(cfg -> billingEngine, "submit", "--force", "--logical-date", "2020-06-16","--processing-group","ALL", "--run-id", "15");
  }

  @Test
  void testLogicalDateParamIsPresent() {
    BillingEngine billingEngine = new BillingEngine(ConfigFactory.load("application-test.conf")) {
      @Override
      public Batch run(Batch batch) {
        assertThat(generateRunId.apply(batch.id), greaterThan(0L));
        return batch;
      }
    };

    Function<Config, CommandLineApp> app = cfg -> billingEngine;

    run(app, "submit", "--force", "--logical-date", "2020-06-16", "--processing-group", "DEFAULT", "AUSTRALIA");
    assertEquals("The logical date provided as param should be in app.", "2020-06-16", billingEngine.logicalDate.toString());
  }

  @Test
  void testLogicalDateAcceptsCtrlMFormat() {
    BillingEngine billingEngine = new BillingEngine(ConfigFactory.load("application-test.conf")) {
      @Override
      public Batch run(Batch batch) {
        return batch;
      }
    };

    Function<Config, CommandLineApp> app = cfg -> billingEngine;

    run(app, "submit", "--force", "--logical-date", "16/06/2020", "--processing-group", "ALL");
    assertEquals("The logical date provided as param should be in app.", "2020-06-16", billingEngine.logicalDate.toString());

    run(app, "submit", "--force", "--logical-date", "30/09/2020", "--processing-group", "ALL");
    assertEquals("The logical date provided as param should be in app.", "2020-09-30", billingEngine.logicalDate.toString());
  }


  @Test
  void testFailWhenLogicalDateParamIsNotPresent() {
    assertThrows(ParameterException.class, () -> BillingEngine.main("submit", "--force"));
  }

  @Test
  void testProcessingGroupsArePresent(){
    BillingEngine billingEngine = new BillingEngine(ConfigFactory.load("application-test.conf")) {
      @Override
      public Batch run(Batch batch) {
        return batch;
      }
    };

    Function<Config, CommandLineApp> app = cfg -> billingEngine;
    String[] groups = new String[]{"JAPAN", "DEFAULT", "AUSTRALIA"};

    run(app, "submit", "--force", "--logical-date", "16/06/2020", "--processing-group", "DEFAULT", "AUSTRALIA", "JAPAN");

    billingEngine.processingGroups.forEach(group -> assertThat(group,is(in(groups))));
  }

  @Test
  void testFailWhenParamDelegate() {
    BillingEngine billingEngine = new BillingEngine(ConfigFactory.load("application-test.conf")) {
      @Override
      public PmsParameterDelegate getParameterDelegate() {
        return new PmsParameterDelegate() {
        };
      }
    };
    Function<Config, CommandLineApp> app = cfg -> billingEngine;

    assertThrows(BillingException.class, () -> run(app, "submit", "--force"));
  }

}