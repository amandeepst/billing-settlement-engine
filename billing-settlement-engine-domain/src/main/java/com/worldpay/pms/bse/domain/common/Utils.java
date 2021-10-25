package com.worldpay.pms.bse.domain.common;

import static java.time.temporal.ChronoUnit.SECONDS;

import com.aventrix.jnanoid.jnanoid.NanoIdUtils;
import com.worldpay.pms.utils.Strings;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

@UtilityClass
public class Utils {

  public static final String FLAG_YES = "Y";
  public static final String BILL_CYCLE_WPDY = "WPDY";

  private static final Random NANO_ID_NUMBER_GENERATOR = new Random();
  private static final char[] NANO_ID_ALPHABET =
      "!$%*+-0123456789:;<=>@ABCDEFGHIJKLMNOPQRSTVWXYZ^_abcdefghijklmnopqrstuvwxyz~#".toCharArray();
  private static final int NANO_ID_SIZE = 15;

  public static LocalDate max(@NonNull LocalDate date, @NonNull LocalDate otherDate) {
    return date.isAfter(otherDate) ? date : otherDate;
  }

  public static LocalDate getLocalDate(Date date) {
    return date == null ? null : date.toLocalDate();
  }

  public static Date getDate(LocalDate localDate) {
    return localDate == null ? null : Date.valueOf(localDate);
  }

  public static LocalDateTime getCurrentDateTimeWithSecondRoundedUp() {
    return LocalDateTime.now().truncatedTo(SECONDS).plus(1, SECONDS);
  }

  public static String generateId(){
    return UUID.randomUUID().toString();
  }

  public static String generateNanoId(){
    return NanoIdUtils.randomNanoId(NANO_ID_NUMBER_GENERATOR, NANO_ID_ALPHABET, NANO_ID_SIZE);
  }

  public static String getOrDefault(String value, Supplier<String> defaultValueSupplier){
    if(Strings.isNotNullOrEmptyOrWhitespace(value)){
      return value;
    }
    return defaultValueSupplier.get();
  }
}
