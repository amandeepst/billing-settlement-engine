package com.worldpay.pms.bse.domain;

import com.worldpay.pms.bse.domain.staticdata.Currency;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class DefaultRoundingService implements RoundingService {

  public static final Short DEFAULT_ROUNDING_SCALE = 2;
  private final Map<String, Short> currenciesMap;

  public DefaultRoundingService(Iterable<Currency> currencies) {
    currenciesMap = Stream.ofAll(currencies).toMap(Currency::getCurrencyCode, Currency::getRoundingScale);
  }

  @Override
  public BigDecimal roundAmount(BigDecimal amount, String currency) {
    return amount.setScale(getRoundingScale(currency), RoundingMode.HALF_UP);
  }

  private Short getRoundingScale(String currency) {
    return currenciesMap.getOrElse(currency, DEFAULT_ROUNDING_SCALE);
  }
}