package com.worldpay.pms.pba.engine.utils;

import static com.typesafe.config.ConfigBeanFactory.create;
import static com.typesafe.config.ConfigFactory.load;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.worldpay.pms.pba.engine.PostBillingConfiguration;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import org.junit.jupiter.api.BeforeEach;

public interface WithDatabase {

  @BeforeEach
  default void init() {
    // load the db info for those that want it
    ConfigFactory.invalidateCaches();
    Config conf = load("application-test.conf");
    bindPostBillingJdbcConfiguration(create(conf.getConfig("db"), JdbcConfiguration.class));
    bindPostBillingConfiguration(create(conf.getConfig("settings"), PostBillingConfiguration.class));
    bindMduJdbcConfiguration(create(load("conf/db.conf").getConfig("mdu"), JdbcConfiguration.class));
    bindMamJdbcConfiguration(create(load("conf/db.conf").getConfig("mam"), JdbcConfiguration.class));

  }

  default void bindPostBillingJdbcConfiguration(JdbcConfiguration conf) {
    // do nothing by default.
  }

  default void bindPostBillingConfiguration(PostBillingConfiguration conf) {
    // do nothing by default.
  }

  default void bindMduJdbcConfiguration(JdbcConfiguration conf) {
    // do nothing by default.
  }

  default void bindMamJdbcConfiguration(JdbcConfiguration conf) {
    // do nothing by default.
  }
}
