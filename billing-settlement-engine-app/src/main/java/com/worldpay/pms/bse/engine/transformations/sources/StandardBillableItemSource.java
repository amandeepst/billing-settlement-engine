package com.worldpay.pms.bse.engine.transformations.sources;

import static com.worldpay.pms.bse.engine.transformations.model.FieldConstants.BILLABLE_ITEM_ID;
import static com.worldpay.pms.bse.engine.transformations.model.FieldConstants.COL_NAME_VALUE;
import static com.worldpay.pms.bse.engine.transformations.model.FieldConstants.MERGE_ARRAY_OF_MAPS;
import static com.worldpay.pms.bse.engine.transformations.model.FieldConstants.OVERRIDE_ACCRUED_DATE;
import static com.worldpay.pms.bse.engine.transformations.model.FieldConstants.PARTITION_ID;
import static com.worldpay.pms.bse.engine.transformations.model.FieldConstants.RATE_SCHEDULE;
import static com.worldpay.pms.bse.engine.transformations.model.FieldConstants.SERVICE_QUANTITIES;
import static com.worldpay.pms.bse.engine.transformations.model.FieldConstants.SERVICE_QUANTITY;
import static com.worldpay.pms.bse.engine.transformations.model.FieldConstants.SQI_CD;
import static com.worldpay.pms.bse.engine.transformations.model.FieldConstants.SVC_QTY_LIST;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.map;

import com.google.common.collect.Iterators;
import com.worldpay.pms.bse.engine.BillingConfiguration.BillableItemSourceConfiguration;
import com.worldpay.pms.bse.engine.BillingConfiguration.FailedBillableItemSourceConfiguration;
import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.InputEncodings;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemRow;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemRow.Fields;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.input.standard.StandardBillableItemLineRow;
import com.worldpay.pms.bse.engine.transformations.model.input.standard.StandardBillableItemRow;
import com.worldpay.pms.bse.engine.transformations.model.input.standard.StandardBillableItemSvcQtyRow;
import com.worldpay.pms.spark.core.DataSource;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import io.vavr.control.Option;
import java.sql.Date;
import lombok.val;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

public class StandardBillableItemSource implements DataSource<BillableItemRow> {

  private final BillingDataSource<StandardBillableItemRow> billableItemRowSource;
  private final BillingDataSource<StandardBillableItemLineRow> billableItemLineRowSource;
  private final BillingDataSource<StandardBillableItemSvcQtyRow> billableItemSvcQtyRowSource;
  private final BillingDataSource<StandardBillableItemRow> failedBillableItemRowSource;
  private final BillingDataSource<StandardBillableItemLineRow> failedBillableItemLineRowSource;
  private final BillingDataSource<StandardBillableItemSvcQtyRow> failedBillableItemSvcQtyRowSource;
  private final Option<Date> overrideAccruedDate;

  public StandardBillableItemSource(BillableItemSourceConfiguration conf, FailedBillableItemSourceConfiguration failedConf,
      Option<Date> overrideAccruedDate, int maxAttempts) {
    this.overrideAccruedDate = overrideAccruedDate;
    this.billableItemRowSource = new BillingDataSource<>("billable-item",
        "sql/inputs/get-billable-items.sql", conf, InputEncodings.STANDARD_BILLABLE_ITEM_ROW_ENCODER);
    this.billableItemLineRowSource = new BillingDataSource<>("billable-item-line",
        "sql/inputs/get-billable-item-lines.sql", conf, InputEncodings.STANDARD_BILLABLE_ITEM_LINE_ROW_ENCODER);
    this.billableItemSvcQtyRowSource = new BillingDataSource<>("billable-item-service-quantity",
        "sql/inputs/get-billable-item-svc-qty.sql", conf, InputEncodings.STANDARD_BILLABLE_ITEM_SVC_QTY_ROW_ENCODER);
    this.failedBillableItemRowSource = new BillingDataSource<>("failed-billable-item",
        "sql/inputs/get-failed-billable-items.sql", failedConf, maxAttempts, InputEncodings.STANDARD_BILLABLE_ITEM_ROW_ENCODER);
    this.failedBillableItemLineRowSource = new BillingDataSource<>("failed-billable-item-line",
        "sql/inputs/get-failed-billable-item-lines.sql", failedConf, maxAttempts, InputEncodings.STANDARD_BILLABLE_ITEM_LINE_ROW_ENCODER);
    this.failedBillableItemSvcQtyRowSource = new BillingDataSource<>("failed-billable-item-service-quantity",
        "sql/inputs/get-failed-billable-item-svc-qty.sql", failedConf, maxAttempts, InputEncodings.STANDARD_BILLABLE_ITEM_SVC_QTY_ROW_ENCODER);
  }

  public Dataset<BillableItemRow> load(SparkSession session, BatchId batchId) {
    val itemRowDs = billableItemRowSource.load(session, batchId)
        .union(failedBillableItemRowSource.load(session, batchId));
    val itemServiceQtyRowDs = billableItemSvcQtyRowSource.load(session, batchId)
        .union(failedBillableItemSvcQtyRowSource.load(session, batchId));
    val itemLineRowDs = billableItemLineRowSource.load(session, batchId)
        .union(failedBillableItemLineRowSource.load(session, batchId));

    val itemServiceQtyDs = mapBillableItemSvcQuantities(itemServiceQtyRowDs);
    val itemLineDs = groupBillableItemLines(itemLineRowDs);
    val itemRowWithLines = groupItemWithItemLines(itemRowDs, itemLineDs);

    val groupedItemsWithSvcQty = groupItemWithLinesWithSvcQty(itemRowWithLines, itemServiceQtyDs);

    return overrideAccruedDate
        .map(date -> groupedItemsWithSvcQty.withColumn(Fields.accruedDate, OVERRIDE_ACCRUED_DATE.apply(col(Fields.accruedDate), lit(date))))
        .map(data -> data.as(Encodings.BILLABLE_ITEM_ROW_ENCODER))
        .getOrElse(groupedItemsWithSvcQty);
  }

  /**
   * Group SQI Code and Service Quantity into a MapType column in order to reduce dataframe size and bind to a single BillableItemSvqQty
   * object
   *
   * @param itemSvcQtyRowDs Raw Service Quantities
   * @return grouped service quantities in a map
   */
  private Dataset<BillableItemServiceQuantityRow> mapBillableItemSvcQuantities(Dataset<StandardBillableItemSvcQtyRow> itemSvcQtyRowDs) {
    return itemSvcQtyRowDs
        .withColumn(SERVICE_QUANTITIES, map(col(SQI_CD), col(SERVICE_QUANTITY)))
        .groupBy(BILLABLE_ITEM_ID, RATE_SCHEDULE, PARTITION_ID)
        .agg(collect_list(SERVICE_QUANTITIES).as(SVC_QTY_LIST))
        .withColumn(SERVICE_QUANTITIES, MERGE_ARRAY_OF_MAPS.apply(col(SVC_QTY_LIST)))
        .drop(SQI_CD, SERVICE_QUANTITY, SVC_QTY_LIST)
        .as(Encodings.BILLABLE_ITEM_SERVICE_QUANTITY_ROW_ENCODER);
  }

  private Dataset<Tuple2<String, StandardBillableItemLineRow[]>> groupBillableItemLines(
      Dataset<StandardBillableItemLineRow> itemLineRowDs) {
    return itemLineRowDs
        .groupByKey(StandardBillableItemLineRow::getBillableItemId, Encoders.STRING())
        .mapGroups(mapBillItemLineToTuple(),
            Encoders.tuple(Encoders.STRING(), InputEncodings.STANDARD_BILLABLE_ITEM_LINE_ROW_ARRAY_ENCODER));
  }

  private Dataset<Tuple3<String, StandardBillableItemRow, StandardBillableItemLineRow[]>> groupItemWithItemLines(
      Dataset<StandardBillableItemRow> itemRowDs,
      Dataset<Tuple2<String, StandardBillableItemLineRow[]>> itemLineDs) {

    return itemRowDs
        .joinWith(itemLineDs, itemRowDs.col(BILLABLE_ITEM_ID).equalTo(itemLineDs.col(COL_NAME_VALUE)))
        .map(x -> new Tuple3<>(x._2._1, x._1, x._2._2),
            Encoders.tuple(Encoders.STRING(), InputEncodings.STANDARD_BILLABLE_ITEM_ROW_ENCODER,
                InputEncodings.STANDARD_BILLABLE_ITEM_LINE_ROW_ARRAY_ENCODER));
  }

  private Dataset<BillableItemRow> groupItemWithLinesWithSvcQty(
      Dataset<Tuple3<String, StandardBillableItemRow, StandardBillableItemLineRow[]>> itemRowWithLines,
      Dataset<BillableItemServiceQuantityRow> itemServiceQtyDs) {

    return itemRowWithLines
        .joinWith(itemServiceQtyDs, itemRowWithLines.col(COL_NAME_VALUE).equalTo(itemServiceQtyDs.col(BILLABLE_ITEM_ID)))
        .map(x -> new Tuple3<>(x._1._2(), x._1._3(), x._2),
            Encoders.tuple(InputEncodings.STANDARD_BILLABLE_ITEM_ROW_ENCODER, InputEncodings.STANDARD_BILLABLE_ITEM_LINE_ROW_ARRAY_ENCODER,
                Encodings.BILLABLE_ITEM_SERVICE_QUANTITY_ROW_ENCODER))
        .map(x -> BillableItemRow.from(x._1(), x._2(), x._3()), Encodings.BILLABLE_ITEM_ROW_ENCODER);
  }

  private MapGroupsFunction<String, StandardBillableItemLineRow, Tuple2<String, StandardBillableItemLineRow[]>> mapBillItemLineToTuple() {
    return (k, iter) -> new Tuple2<>(k, Iterators.toArray(iter, StandardBillableItemLineRow.class));
  }
}
