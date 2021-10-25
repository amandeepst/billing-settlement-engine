package com.worldpay.pms.bse.domain.common;

import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import org.junit.jupiter.api.Test;

class UtilsTest {

  @Test
  void whenMaxBetweenEqualDatesThenReturnThatDate() {
    assertThat(LocalDate.parse("2021-02-08"), equalTo(Utils.max(LocalDate.parse("2021-02-08"), LocalDate.parse("2021-02-08"))));
  }

  @Test
  void whenMaxBetweenNonEqualDatesThenReturnTheLaterOne() {
    assertThat(LocalDate.parse("2021-02-08"), equalTo(Utils.max(LocalDate.parse("2021-02-08"), LocalDate.parse("2021-02-07"))));
    assertThat(LocalDate.parse("2021-02-08"), equalTo(Utils.max(LocalDate.parse("2021-02-07"), LocalDate.parse("2021-02-08"))));
  }

  @Test
  void whenDateIsNotNullThenReturnNotNullLocalDate() {
    assertThat(Date.valueOf("2021-02-08"), equalTo(Utils.getDate(LocalDate.parse("2021-02-08"))));
  }

  @Test
  void whenDateIsNullThenReturnNull() {
    assertNull(Utils.getLocalDate(null));
  }

  @Test
  void whenLocalDateIsNotNullThenReturnNotNullDate() {
    assertThat(LocalDate.parse("2021-02-08"), equalTo(Utils.getLocalDate(Date.valueOf("2021-02-08"))));
  }

  @Test
  void whenLocalDateIsNullThenReturnNull() {
    assertNull(Utils.getDate(null));
  }

  @Test
  void whenGenerateIdThenReturnValue() {
    assertNotNull(Utils.generateId());
  }

  @Test
  void whenGetOrDefaultThenReturnCorrectValue() {
    assertThat(Utils.getOrDefault("abc", ()-> "cde"), is("abc"));
    assertThat(Utils.getOrDefault(null, ()-> "cde"), is("cde"));
  }

  @Test
  void whenGenerateNanoIdThenGenerate15CharsId() {
    long generations = 1000000;
    HashMap<String, Short> ids = new HashMap<>();
    for (int i = 1; i <= generations; i++) {
      String id = Utils.generateNanoId();
      assertThat(id.length(), is(15));
      if(ids.containsKey(id)) {
        fail(String.format("Nano id collision after %s generations: %s", i, id));
      }
      ids.put(id, (short)1);
    }
  }

  @Test
  void whenGetCurrentTimeWithSecondRoundedUpThenResultHasNoMillisAndIsMax1SecondGreaterThanCurrentTime() {
    LocalDateTime currentTime = LocalDateTime.now();
    LocalDateTime currentTimeWithSecondRoundedUp = Utils.getCurrentDateTimeWithSecondRoundedUp();

    assertThat(currentTimeWithSecondRoundedUp.get(MILLI_OF_SECOND), is(0));
    assertThat(currentTime.isBefore(currentTimeWithSecondRoundedUp), is(true));
    assertThat(SECONDS.between(currentTime, currentTimeWithSecondRoundedUp) <= 1, is(true));
  }
}