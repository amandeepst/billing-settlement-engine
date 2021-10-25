package com.worldpay.pms.bse.engine.transformations.sources;

import static com.worldpay.pms.bse.engine.transformations.model.FieldConstants.COL_NAME_VALUE;
import static com.worldpay.pms.bse.engine.transformations.model.FieldConstants.JOIN_TYPE_INNER;

import com.google.common.collect.Iterators;
import com.worldpay.pms.bse.engine.BillingConfiguration.BillCorrectionSourceConfiguration;
import com.worldpay.pms.bse.engine.InputCorrectionEncodings;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillCorrectionRow;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillLineCalcCorrectionRow;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillLineCorrectionRow;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillLineSvcQtyCorrectionRow;
import com.worldpay.pms.spark.core.DataSource;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import lombok.val;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

public class BillCorrectionSource implements DataSource<Tuple2<InputBillCorrectionRow, BillLineRow[]>> {

  private static final String QUERY_BILL_CORRECTIONS = "sql/inputs/get-bill-corrections.sql";
  private static final String QUERY_BILL_LINE_CORRECTIONS = "sql/inputs/get-bill-line-corrections.sql";
  private static final String QUERY_BILL_LINE_CALC_CORRECTIONS = "sql/inputs/get-bill-line-calc-corrections.sql";
  private static final String QUERY_BILL_LINE_SVC_QTY_CORRECTIONS = "sql/inputs/get-bill-line-svc-qty-corrections.sql";

  private final BillingDataSource<InputBillCorrectionRow> billCorrSource;
  private final BillingDataSource<InputBillLineCorrectionRow> billLineCorrectionSource;
  private final BillingDataSource<InputBillLineCalcCorrectionRow> billLineCalcCorrectionSource;
  private final BillingDataSource<InputBillLineSvcQtyCorrectionRow> billLineSvcQtyCorrectionSource;

  public BillCorrectionSource(BillCorrectionSourceConfiguration conf) {
    billCorrSource = new BillingDataSource<>("bill-correction", QUERY_BILL_CORRECTIONS,
        conf.getBillCorrection(), InputCorrectionEncodings.INPUT_BILL_CORRECTION_ROW_ENCODER);
    billLineCorrectionSource = new BillingDataSource<>("bill-line-correction", QUERY_BILL_LINE_CORRECTIONS,
        conf.getBillLineCorrection(), InputCorrectionEncodings.INPUT_BILL_LINE_CORRECTION_ROW_ENCODER);
    billLineCalcCorrectionSource = new BillingDataSource<>("bill-line-calc-correction", QUERY_BILL_LINE_CALC_CORRECTIONS,
        conf.getBillLineCalcCorrection(), InputCorrectionEncodings.INPUT_BILL_LINE_CALC_CORRECTION_ROW_ENCODER);
    billLineSvcQtyCorrectionSource = new BillingDataSource<>("bill-line-svc-qty-correction", QUERY_BILL_LINE_SVC_QTY_CORRECTIONS,
        conf.getBillLineSvcQtyCorrection(), InputCorrectionEncodings.INPUT_BILL_LINE_SVC_QTY_CORRECTION_ROW_ENCODER);
  }

  @Override
  public Dataset<Tuple2<InputBillCorrectionRow, BillLineRow[]>> load(SparkSession sparkSession, BatchId batchId) {
    val billCorrections = billCorrSource.load(sparkSession, batchId);
    val lineCorrections = billLineCorrectionSource.load(sparkSession, batchId);
    val lineCalcCorrections = billLineCalcCorrectionSource.load(sparkSession, batchId);
    val lineSvcQtyCorrections = billLineSvcQtyCorrectionSource.load(sparkSession, batchId);

    val groupedBillLineCalcs = groupBillLineCalcByBillLineId(lineCalcCorrections);
    val groupedBillLineSvcQty = groupBillLineSvcQtByBillLineId(lineSvcQtyCorrections);
    val groupedCalcAndSvcQty =
        joinCalcAndSvcQtyWithSameBillLineId(groupedBillLineCalcs, groupedBillLineSvcQty);
    val groupedBillLine = groupBillLinesByBillId(lineCorrections, groupedCalcAndSvcQty);

    return createBillsAndJoinWithLines(billCorrections, groupedBillLine);
  }

  private Dataset<Tuple2<InputBillCorrectionRow, BillLineRow[]>> createBillsAndJoinWithLines(Dataset<InputBillCorrectionRow> billCorrection,
      Dataset<Tuple2<String, BillLineRow[]>> groupedBillLine) {

    return billCorrection
        .joinWith(groupedBillLine, billCorrection.col("billId").equalTo(groupedBillLine.col(COL_NAME_VALUE)), "left")
        .map(t -> new Tuple2(t._1, t._2 == null ? null : t._2._2()), InputCorrectionEncodings.INPUT_BILL_ROW_WITH_LINES_ENCODER);
  }

  private Dataset<Tuple2<String, BillLineRow[]>> groupBillLinesByBillId(Dataset<InputBillLineCorrectionRow> lineCorrections,
      Dataset<Tuple3<String, BillLineCalculationRow[], BillLineServiceQuantityRow[]>> groupedCalcAndSvcQty) {

    return lineCorrections
        .joinWith(groupedCalcAndSvcQty, lineCorrections.col("billLineId").equalTo(groupedCalcAndSvcQty.col(COL_NAME_VALUE)),
            JOIN_TYPE_INNER)
        .map(t -> {
          InputBillLineCorrectionRow billLineCorrection = t._1();
          BillLineCalculationRow[] billLineCalcCorrections = t._2._2();
          BillLineServiceQuantityRow[] billLineSvcQtyCorrections = t._2._3();

          return new Tuple3<>(billLineCorrection, billLineCalcCorrections, billLineSvcQtyCorrections);
        }, Encoders.tuple(InputCorrectionEncodings.INPUT_BILL_LINE_CORRECTION_ROW_ENCODER,
            InputCorrectionEncodings.BILL_LINE_CALC_CORRECTION_ROW_ARRAY_ENCODER,
            InputCorrectionEncodings.BILL_LINE_SVC_QTY_CORRECTION_ROW_ARRAY_ENCODER))
        .groupByKey(tuple -> tuple._1().getBillId(), Encoders.STRING())
        .mapValues(tuple -> BillLineRow.from(tuple._1(), tuple._2(), tuple._3()), InputCorrectionEncodings.BILL_LINE_CORRECTION_ROW_ENCODER)
        .mapGroups(mapKeyAndBillLineIteratorToTuple(),
            Encoders.tuple(Encoders.STRING(), InputCorrectionEncodings.BILL_LINE_CORRECTION_ROW_ARRAY_ENCODER));
  }

  private Dataset<Tuple3<String, BillLineCalculationRow[], BillLineServiceQuantityRow[]>> joinCalcAndSvcQtyWithSameBillLineId(
      Dataset<Tuple2<String, BillLineCalculationRow[]>> groupedBillLineCalcs,
      Dataset<Tuple2<String, BillLineServiceQuantityRow[]>> groupedBillLineSvcQty) {
    return groupedBillLineCalcs
        .joinWith(groupedBillLineSvcQty, groupedBillLineCalcs.col(COL_NAME_VALUE).equalTo(groupedBillLineSvcQty.col(COL_NAME_VALUE)),
            JOIN_TYPE_INNER)
        .map(t -> {
              String billLineId = t._1._1;
              BillLineCalculationRow[] billLineCalcCorrections = t._1._2;
              BillLineServiceQuantityRow[] billLineSvcQtyCorrections = t._2._2;

              return new Tuple3<>(billLineId, billLineCalcCorrections, billLineSvcQtyCorrections);
            },
            Encoders.tuple(Encoders.STRING(), InputCorrectionEncodings.BILL_LINE_CALC_CORRECTION_ROW_ARRAY_ENCODER,
                InputCorrectionEncodings.BILL_LINE_SVC_QTY_CORRECTION_ROW_ARRAY_ENCODER));
  }

  private Dataset<Tuple2<String, BillLineServiceQuantityRow[]>> groupBillLineSvcQtByBillLineId(
      Dataset<InputBillLineSvcQtyCorrectionRow> lineSvcQtyCorrections) {
    return lineSvcQtyCorrections
        .groupByKey(InputBillLineSvcQtyCorrectionRow::getBillLineId, Encoders.STRING())
        .mapValues(BillLineServiceQuantityRow::from, InputCorrectionEncodings.BILL_LINE_SVC_QTY_CORRECTION_ROW_ENCODER)
        .mapGroups(mapKeyAndPendingBillLineSvcQtyIteratorToTuple(),
            Encoders.tuple(Encoders.STRING(), InputCorrectionEncodings.BILL_LINE_SVC_QTY_CORRECTION_ROW_ARRAY_ENCODER));
  }

  private Dataset<Tuple2<String, BillLineCalculationRow[]>> groupBillLineCalcByBillLineId(
      Dataset<InputBillLineCalcCorrectionRow> lineCalcCorrections) {
    return lineCalcCorrections
        .groupByKey(InputBillLineCalcCorrectionRow::getBillLineId, Encoders.STRING())
        .mapValues(BillLineCalculationRow::from, InputCorrectionEncodings.BILL_LINE_CALC_CORRECTION_ROW_ENCODER)
        .mapGroups(mapKeyAndBillLineCalcIteratorToTuple(),
            Encoders.tuple(Encoders.STRING(), InputCorrectionEncodings.BILL_LINE_CALC_CORRECTION_ROW_ARRAY_ENCODER));
  }

  private MapGroupsFunction<String, BillLineCalculationRow, Tuple2<String, BillLineCalculationRow[]>>
  mapKeyAndBillLineCalcIteratorToTuple() {
    return (k, iter) -> new Tuple2<>(k, Iterators.toArray(iter, BillLineCalculationRow.class));
  }

  private MapGroupsFunction<String, BillLineServiceQuantityRow, Tuple2<String, BillLineServiceQuantityRow[]>>
  mapKeyAndPendingBillLineSvcQtyIteratorToTuple() {
    return (k, iter) -> new Tuple2<>(k, Iterators.toArray(iter, BillLineServiceQuantityRow.class));
  }

  private MapGroupsFunction<String, BillLineRow, Tuple2<String, BillLineRow[]>> mapKeyAndBillLineIteratorToTuple() {
    return (k, iter) -> new Tuple2<>(k, Iterators.toArray(iter, BillLineRow.class));
  }
}
