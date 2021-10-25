package com.worldpay.pms.bse.engine.transformations.sources;

import static org.apache.spark.sql.Encoders.STRING;
import static org.apache.spark.sql.Encoders.tuple;

import com.google.common.collect.Iterators;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTaxDetail;
import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.InputEncodings;
import com.worldpay.pms.bse.engine.transformations.model.input.billtax.InputBillTaxDetailRow;
import com.worldpay.pms.bse.engine.transformations.model.input.billtax.InputBillTaxRow;
import com.worldpay.pms.spark.core.DataSource;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import com.worldpay.pms.spark.core.jdbc.JdbcSourceConfiguration;
import lombok.val;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class BillTaxSource implements DataSource<BillTax> {

  private final BillingDataSource<InputBillTaxRow> billTaxDataSource;
  private final BillingDataSource<InputBillTaxDetailRow> billTaxDetailDataSource;

  public BillTaxSource(JdbcSourceConfiguration conf) {
    billTaxDataSource = new BillingDataSource<>("bill_tax", "sql/inputs/get-bill-taxes.sql", conf, InputEncodings.INPUT_BILL_TAX_ROW);
    billTaxDetailDataSource = new BillingDataSource<>("bill_tax_detail", "sql/inputs/get-bill-tax-details.sql", conf,
        InputEncodings.INPUT_BILL_TAX_DETAIL_ROW);
  }

  @Override
  public Dataset<BillTax> load(SparkSession sparkSession, BatchId batchId) {
    val billTaxes = billTaxDataSource.load(sparkSession, batchId);
    val billTaxDetails = billTaxDetailDataSource.load(sparkSession, batchId);

    val groupedBillTaxDetails = billTaxDetails
        .map(InputBillTaxDetailRow::toBillTaxDetail, Encodings.BILL_TAX_DETAIL)
        .groupByKey(BillTaxDetail::getBillTaxId, STRING())
        .mapGroups(mapKeyAndBillTaxDetailsToTuple(), tuple(STRING(), Encodings.BILL_TAX_DETAIL_ARRAY));

    return billTaxes
        .joinWith(groupedBillTaxDetails, billTaxes.col("billTaxId").equalTo(groupedBillTaxDetails.col("value")))
        .map(t -> t._1.toBillTax(t._2._2), Encodings.BILL_TAX);
  }

  private MapGroupsFunction<String, BillTaxDetail, Tuple2<String, BillTaxDetail[]>> mapKeyAndBillTaxDetailsToTuple() {
    return (k, iter) -> new Tuple2<>(k, Iterators.toArray(iter, BillTaxDetail.class));
  }
}
