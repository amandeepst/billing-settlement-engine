package com.worldpay.pms.pba.engine.common;

import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;

class UtilsTest {

  @Test
  void whenGetCurrentTimeWithSecondRoundedUpThenResultHasNoMillisAndIsMax1SecondGreaterThanCurrentTime() {
    LocalDateTime currentTime = LocalDateTime.now();
    LocalDateTime currentTimeWithSecondRoundedUp = Utils.getCurrentDateTimeWithSecondRoundedUp();

    assertThat(currentTimeWithSecondRoundedUp.get(MILLI_OF_SECOND), is(0));
    assertThat(currentTime.isBefore(currentTimeWithSecondRoundedUp), is(true));
    assertThat(SECONDS.between(currentTime, currentTimeWithSecondRoundedUp) <= 1, is(true));
  }
}