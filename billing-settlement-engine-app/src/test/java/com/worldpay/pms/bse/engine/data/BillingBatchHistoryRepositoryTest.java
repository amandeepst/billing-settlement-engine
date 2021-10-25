package com.worldpay.pms.bse.engine.data;

import static com.worldpay.pms.bse.engine.data.BillingBatchHistoryRepository.interpolateHints;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class BillingBatchHistoryRepositoryTest {

  private static final String SQL_EXPR = "/*+ :insert-hints */|/*+ :select-hints */";

  @ParameterizedTest
  @CsvSource({
      "a:b,/*+ a */|/*+ b */",
      ":b,/*+  */|/*+ b */",
      "a:,/*+ a */|/*+  */",
      "a,/*+ a */|/*+  */",
      "'',/*+  */|/*+  */",
      ",/*+  */|/*+  */",
  })
  void cantInterpolateHintsForInsertAndSelect(String hints, String expected) {
    assertThat(interpolateHints(SQL_EXPR, hints), is(expected));
  }
}
