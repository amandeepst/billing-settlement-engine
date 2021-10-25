package com.worldpay.pms.bse.domain;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


import com.worldpay.pms.bse.domain.staticdata.Currency;
import io.vavr.collection.List;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class DefaultRoundingServiceTest {

  private static final Currency EUR = new Currency("EUR", (short) 2);
  private static final Currency USD = new Currency("USD", (short) 3);
  private static final Currency RON = new Currency("RON", (short) 4);

  private final DefaultRoundingService defaultRoundingService = new DefaultRoundingService(List.of(EUR, USD, RON));

  @Test
  void whenCurrencyIsPresentInRoundingServiceThenScaleAndRoundingOfTheResultIsCorrect() {
    assertThat(defaultRoundingService.roundAmount(BigDecimal.valueOf(8.012345), "EUR").toPlainString(), is("8.01"));
    assertThat(defaultRoundingService.roundAmount(BigDecimal.valueOf(9.012545), "USD").toPlainString(), is("9.013"));
    assertThat(defaultRoundingService.roundAmount(BigDecimal.valueOf(10.0123511), "RON").toPlainString(), is("10.0124"));
  }

  @Test
  void testRoundingServiceWhenCurrencyIsNotPresent() {
    assertThat(defaultRoundingService.roundAmount(BigDecimal.valueOf(0.012345), "JPY").toPlainString(), is("0.01"));
    assertThat(defaultRoundingService.roundAmount(BigDecimal.valueOf(0.01500001), "JPY").toPlainString(), is("0.02"));
  }
}