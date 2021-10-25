package com.worldpay.pms.bse.domain.account;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.vavr.collection.List;
import io.vavr.control.Option;
import java.time.LocalDate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class BillingCycleTreeMapTest {

  private static List<BillingCycle> BILLING_CYCLES = List.of(
      new BillingCycle("WPMO", LocalDate.of(2021, 1, 1), LocalDate.of(2021, 1, 31)),
      new BillingCycle("WPMO", LocalDate.of(2021, 2, 1), LocalDate.of(2021, 2, 28)),
      new BillingCycle("WPDY", LocalDate.of(2021, 1, 10), LocalDate.of(2021, 1, 10)),
      new BillingCycle("WPDY", LocalDate.of(2021, 1, 11), LocalDate.of(2021, 1, 11))
  );

  private BillingCycleTreeMap billingCycleMap;

  @BeforeEach
  void setUp() {
    billingCycleMap = new BillingCycleTreeMap(BILLING_CYCLES);
  }

  @Test
  @DisplayName("When get billing cycle that ends after date then return correct value")
  void whenGetBillingCycleThatEndsAfterDateThenReturnCorrectValue() {
    BillingCycle actual = billingCycleMap.getBillingCycleThatEndsAfterDate("WPMO",  LocalDate.of(2020, 12, 10))
        .get();
    BillingCycle expected =  new BillingCycle("WPMO", LocalDate.of(2021, 1, 1), LocalDate.of(2021, 1, 31));

    assertThat(actual, is(expected));
  }

  @Test
  @DisplayName("When get billing cycle that ends after date and no result found then return empty")
  void whenGetBillingCycleThatEndsAfterDateAndNoResultThenReturnEmpty() {
    Option<BillingCycle> actual = billingCycleMap.getBillingCycleThatEndsAfterDate("WPMO",  LocalDate.of(2021, 3, 10));
    assertThat(actual.isEmpty(), is(true));
  }

  @Test
  @DisplayName("When get billing cycle that contains date then return correct value")
  void whenGetBillingCycleTatContainsDateThenReturnCorrectValue() {
    BillingCycle actual = billingCycleMap.getBillingCycleThatContainsDate("WPMO",  LocalDate.of(2021, 1, 10))
        .get();
    BillingCycle expected =  new BillingCycle("WPMO", LocalDate.of(2021, 1, 1), LocalDate.of(2021, 1, 31));

    assertThat(actual, is(expected));
  }

  @Test
  @DisplayName("When get billing cycle that contains date for the start of the bill cycle then return correct value")
  void whenGetBillingCycleTatContainsDateForWinStartThenReturnCorrectValue() {
    BillingCycle actual = billingCycleMap.getBillingCycleThatContainsDate("WPMO",  LocalDate.of(2021, 1, 1))
        .get();
    BillingCycle expected =  new BillingCycle("WPMO", LocalDate.of(2021, 1, 1), LocalDate.of(2021, 1, 31));

    assertThat(actual, is(expected));
  }

  @Test
  @DisplayName("When get billing cycle that contains date for the end of the bill cycle then return correct value")
  void whenGetBillingCycleTatContainsDateForWinEndThenReturnCorrectValue() {
    BillingCycle actual = billingCycleMap.getBillingCycleThatContainsDate("WPDY",  LocalDate.of(2021, 1, 10))
        .get();
    BillingCycle expected =  new BillingCycle("WPDY", LocalDate.of(2021, 1, 10), LocalDate.of(2021, 1, 10));

    assertThat(actual, is(expected));
  }

  @Test
  @DisplayName("When return bill cycle that contains date and no value found then return empty")
  void whenNoBillCycleThatContainsDateFoundThenReturnEmpty() {
    Option<BillingCycle> actual = billingCycleMap.getBillingCycleThatContainsDate("WPMO",  LocalDate.of(2020, 12, 10));
    assertThat(actual.isEmpty(), is(true));
  }

  @Test
  @DisplayName("When get bill cycle for an invalid code then return empty")
  void whenNoBillCyleFoundForCodeThenReturnEmpty() {
    Option<BillingCycle> actual = billingCycleMap.getBillingCycleThatContainsDate("ABCD",  LocalDate.of(2020, 12, 10));
    assertThat(actual.isEmpty(), is(true));
  }
}
