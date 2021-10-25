package com.worldpay.pms.bse.engine.transformations.model;

import static org.apache.spark.sql.functions.udf;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.HashMap;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;
import scala.collection.mutable.WrappedArray;

@UtilityClass
public final class FieldConstants {

  // Billable Items
  public static final String SVC_QTY_LIST = "svcQtyList";
  public static final String SERVICE_QUANTITIES = "serviceQuantities";
  public static final String SERVICE_QUANTITY = "serviceQuantity";
  public static final String SQI_CD = "sqiCd";
  public static final String RATE_SCHEDULE = "rateSchedule";
  public static final String BILLABLE_ITEM_ID = "billableItemId";
  public static final String PARTITION_ID = "partitionId";
  public static final int DECIMAL_PRECISION = 38;
  public static final int DECIMAL_SCALE = 18;
  public static final int IGNORED_RETRY_COUNT = 999;
  public static final int DUPLICATED_RETRY_COUNT = 998;
  public static final String COL_NAME_VALUE = "value";
  public static final String JOIN_TYPE_INNER = "inner";

  /**
   * User defined function to merge a List of individual Maps into a single Map.
   */
  public static final UserDefinedFunction MERGE_ARRAY_OF_MAPS = udf((WrappedArray<Map<String, BigDecimal>> mapList) -> {
    java.util.Map<String, BigDecimal> combinedMaps = new HashMap<>(6);
    JavaConverters.asJavaCollectionConverter(mapList).asJavaCollection()
        .forEach(map -> combinedMaps.putAll(JavaConverters.mapAsJavaMapConverter(map).asJava()));
    return JavaConverters.mapAsScalaMapConverter(combinedMaps).asScala();
  }, DataTypes.createMapType(DataTypes.StringType, DataTypes.createDecimalType(DECIMAL_PRECISION, DECIMAL_SCALE)));

  /**
   * User defined function to override accrued date with max(accruedDate, logicalDate)
   */
  public static final UserDefinedFunction OVERRIDE_ACCRUED_DATE = udf(
        (Date accruedDate, Date logicalDate) -> logicalDate.after(accruedDate) ? logicalDate : accruedDate
        , DataTypes.DateType);
}
