package com.worldpay.pms.bse.engine.transformations.sources;

import static com.worldpay.pms.bse.engine.transformations.model.FieldConstants.OVERRIDE_ACCRUED_DATE;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import com.google.common.collect.Iterators;
import com.worldpay.pms.bse.engine.BillingConfiguration.FailedBillableItemSourceConfiguration;
import com.worldpay.pms.bse.engine.BillingConfiguration.MiscBillableItemSourceConfiguration;
import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.InputEncodings;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemRow;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemRow.Fields;
import com.worldpay.pms.bse.engine.transformations.model.input.misc.MiscBillableItemLineRow;
import com.worldpay.pms.bse.engine.transformations.model.input.misc.MiscBillableItemRow;
import com.worldpay.pms.spark.core.DataSource;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import io.vavr.control.Option;
import java.sql.Date;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class MiscBillableItemSource implements DataSource<BillableItemRow> {

  private final BillingDataSource<MiscBillableItemRow> miscBillableItemRowSource;
  private final BillingDataSource<MiscBillableItemLineRow> miscBillableItemLineRowSource;
  private final BillingDataSource<MiscBillableItemRow> failedMiscBillableItemRowSource;
  private final BillingDataSource<MiscBillableItemLineRow> failedMiscBillableItemLineRowSource;
  private final Option<Date> overrideAccruedDate;

  public MiscBillableItemSource(MiscBillableItemSourceConfiguration conf, FailedBillableItemSourceConfiguration failedConf,
      Option<Date> overrideAccruedDate, int maxAttempts) {
    this.overrideAccruedDate = overrideAccruedDate;
    this.miscBillableItemRowSource = new BillingDataSource<>("misc-billable-item",
        "sql/inputs/get-misc-billable-items.sql", conf, InputEncodings.MISC_BILLABLE_ITEM_ROW_ENCODER);
    this.miscBillableItemLineRowSource = new BillingDataSource<>("misc-billable-item-line",
        "sql/inputs/get-misc-billable-item-lines.sql", conf, InputEncodings.MISC_BILLABLE_ITEM_LINE_ROW_ENCODER);
    this.failedMiscBillableItemRowSource = new BillingDataSource<>("failed-misc-billable-item",
        "sql/inputs/get-failed-misc-billable-items.sql", failedConf, maxAttempts, InputEncodings.MISC_BILLABLE_ITEM_ROW_ENCODER);
    this.failedMiscBillableItemLineRowSource = new BillingDataSource<>("failed-misc-billable-item-line",
        "sql/inputs/get-failed-misc-billable-item-lines.sql", failedConf, maxAttempts, InputEncodings.MISC_BILLABLE_ITEM_LINE_ROW_ENCODER);
  }

  @Override
  public Dataset<BillableItemRow> load(SparkSession session, BatchId batchId) {
    val billableItems = miscBillableItemRowSource.load(session, batchId)
        .union(failedMiscBillableItemRowSource.load(session, batchId));
    val billableItemLines = miscBillableItemLineRowSource.load(session, batchId)
        .union(failedMiscBillableItemLineRowSource.load(session, batchId));

    val groupedBillableItemLines = billableItemLines
        .groupByKey(MiscBillableItemLineRow::getBillableItemId, Encoders.STRING())
        .mapGroups((k, iter) -> new Tuple2<>(k, Iterators.toArray(iter, MiscBillableItemLineRow.class)),
            Encoders.tuple(Encoders.STRING(), InputEncodings.MISC_BILLABLE_ITEM_LINE_ROW_ARRAY_ENCODER));

    val billableItemsWithLines = billableItems
        .joinWith(groupedBillableItemLines, billableItems.col("billableItemId").equalTo(groupedBillableItemLines.col("value")))
        .map(tuple -> BillableItemRow.from(tuple._1, tuple._2._2), Encodings.BILLABLE_ITEM_ROW_ENCODER);

    return overrideAccruedDate
        .map(date -> billableItemsWithLines.withColumn(Fields.accruedDate, OVERRIDE_ACCRUED_DATE.apply(col(Fields.accruedDate), lit(date))))
        .map(data -> data.as(Encodings.BILLABLE_ITEM_ROW_ENCODER))
        .getOrElse(billableItemsWithLines);
  }
}
