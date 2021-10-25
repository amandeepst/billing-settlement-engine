package com.worldpay.pms.bse.engine.transformations;

import static com.worldpay.pms.bse.engine.transformations.TransformationUtils.addNullableAmounts;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.worldpay.pms.testing.stereotypes.WithSpark;
import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

@WithSpark
class TransformationUtilsTest {

  @Test
  void whenAddTwoNullValuesReturnNull() {
    BigDecimal result = addNullableAmounts(null, null);
    assertThat(result, is(nullValue()));
  }

  @Test
  void whenAddTwoValuesAndSecondIsNullReturnValue() {
    BigDecimal result = addNullableAmounts(BigDecimal.ONE, null);
    assertThat(result, is(BigDecimal.ONE));
  }

  @Test
  void whenAddTwoValuesAndFirstIsNullReturnValue() {
    BigDecimal result = addNullableAmounts(null, BigDecimal.ONE);
    assertThat(result, is(BigDecimal.ONE));
  }

  @Test
  void whenAddTwoValuesAndBothNotNullReturnValue() {
    BigDecimal result = addNullableAmounts(BigDecimal.ONE, BigDecimal.ONE);
    assertThat(result, is(BigDecimal.valueOf(2)));
  }

  @Test
  void testEnhanceWithUUIds(SparkSession spark) {
    Dataset<Integer> intDs = spark.createDataset(Arrays.asList(1, 2, 3), Encoders.INT());
    Dataset<Tuple2<String, Integer>> dsWithIds = TransformationUtils.enrichDatasetWithUUIds(intDs, Encoders.INT());

    assertThat(dsWithIds.collectAsList().stream().map(Tuple2::_1).allMatch(s -> s.length() == 36), is(true));
  }

  @Test
  void testEnhanceWithIds(SparkSession spark) {
    int idLength = 15;
    Dataset<Integer> intDs = spark.createDataset(Arrays.asList(1, 2, 3), Encoders.INT());
    Dataset<Tuple2<String, Integer>> dsWithIds = TransformationUtils
        .enrichDatasetWithIdsOfSpecificLength(intDs, 1L, idLength, Encoders.INT());

    assertThat(dsWithIds.collectAsList().stream().map(Tuple2::_1).allMatch(s -> s.length() == idLength), is(true));
  }

}