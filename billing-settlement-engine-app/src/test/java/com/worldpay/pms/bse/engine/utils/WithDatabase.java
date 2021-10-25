package com.worldpay.pms.bse.engine.utils;

import static com.typesafe.config.ConfigBeanFactory.create;
import static com.typesafe.config.ConfigFactory.load;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.worldpay.pms.bse.engine.BillingConfiguration;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import org.junit.jupiter.api.BeforeEach;

public interface WithDatabase {

  @BeforeEach
  default void init() {
    // load the db info for those that want it
    ConfigFactory.invalidateCaches();
    Config conf = load("application-test.conf");
    bindPricingJdbcConfiguration(create(load("conf/db.conf").getConfig("pricing"), JdbcConfiguration.class));
    bindChargeUploadJdbcConfiguration(create(load("conf/db.conf").getConfig("chargeUpload"), JdbcConfiguration.class));
    bindBillingJdbcConfiguration(create(conf.getConfig("db"), JdbcConfiguration.class));
    bindBillingConfiguration(create(conf.getConfig("settings"), BillingConfiguration.class));
    bindMduJdbcConfiguration(create(load("conf/db.conf").getConfig("merchantData"), JdbcConfiguration.class));
    bindCisadmJdbcConfiguration(create(load("conf/db.conf").getConfig("cisadm"), JdbcConfiguration.class));
    bindRefDataJdbcConfiguration(create(load("conf/db.conf").getConfig("ref"), JdbcConfiguration.class));
  }

  default void bindPricingJdbcConfiguration(JdbcConfiguration conf) {
    // do nothing by default.
  }

  default void bindChargeUploadJdbcConfiguration(JdbcConfiguration conf) {
    // do nothing by default.
  }

  default void bindBillingJdbcConfiguration(JdbcConfiguration conf) {
    // do nothing by default.
  }

  default void bindBillingConfiguration(BillingConfiguration conf) {
    // do nothing by default.
  }

  default void bindMduJdbcConfiguration(JdbcConfiguration conf) {
    // do nothing by default.
  }

  default void bindCisadmJdbcConfiguration(JdbcConfiguration conf) {
    // do nothing by default.
  }

  default void bindRefDataJdbcConfiguration(JdbcConfiguration conf) {
    // do nothing by default.
  }


}
