package com.worldpay.pms.bse.engine.data;

import com.univocity.parsers.common.record.Record;
import com.worldpay.pms.bse.domain.exception.BillingException;
import com.worldpay.pms.utils.CsvUtils;
import io.vavr.collection.List;
import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public class DataUtils {

  private static final String DATE_PATTERN = "MM/dd/yyyy";

  public static List<Record> getStaticDataCsvRecords(String csvFileName) {
    return List.ofAll(CsvUtils.getCsvRecords("csv/static-data/" + csvFileName)).removeAt(0);
  }

  public static Date getDateOrDefault(String date, String defaultValue) {
    Date defaultDate = Date.valueOf(defaultValue);
    if (date == null || date.isEmpty()) {
      return defaultDate;
    }

    try {
      return Date.valueOf(LocalDate.parse(date, DateTimeFormatter.ofPattern(DATE_PATTERN)));
    } catch (DateTimeParseException ex) {
      throw new BillingException("Couldn't parse date: " + date + ". Required format: " + DATE_PATTERN, ex);
    }
  }

}
