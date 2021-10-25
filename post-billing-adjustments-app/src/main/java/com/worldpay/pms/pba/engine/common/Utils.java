package com.worldpay.pms.pba.engine.common;

import static java.time.temporal.ChronoUnit.SECONDS;

import java.time.LocalDateTime;
import lombok.experimental.UtilityClass;

@UtilityClass
public class Utils {

  public static LocalDateTime getCurrentDateTimeWithSecondRoundedUp() {
    return LocalDateTime.now().truncatedTo(SECONDS).plus(1, SECONDS);
  }
}