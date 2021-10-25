package com.worldpay.pms.bse.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

import com.worldpay.pms.bse.engine.utils.WithDatabase;
import org.junit.jupiter.api.Test;

public class BillingConfigurationTest implements WithDatabase {

  private BillingConfiguration settings;

  @Override
  public void bindBillingConfiguration(BillingConfiguration conf) {
    this.settings = conf;
  }

  @Test
  void passwordsAreNotPrinted() {
    assertThat(settings.toString().toLowerCase(), not(containsString("password")));
  }
}