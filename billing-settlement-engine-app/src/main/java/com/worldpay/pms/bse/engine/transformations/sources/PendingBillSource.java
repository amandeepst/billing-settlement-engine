package com.worldpay.pms.bse.engine.transformations.sources;

import static com.worldpay.pms.bse.engine.transformations.model.FieldConstants.COL_NAME_VALUE;
import static com.worldpay.pms.bse.engine.transformations.model.FieldConstants.JOIN_TYPE_INNER;

import com.google.common.collect.Iterators;
import com.worldpay.pms.bse.engine.BillingConfiguration.PendingConfiguration;
import com.worldpay.pms.bse.engine.Encodings;
import com.worldpay.pms.bse.engine.InputEncodings;
import com.worldpay.pms.bse.engine.transformations.model.input.pending.InputPendingBillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.input.pending.InputPendingBillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.input.pending.InputPendingBillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.input.pending.InputPendingBillRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import com.worldpay.pms.spark.core.DataSource;
import com.worldpay.pms.spark.core.batch.Batch.BatchId;
import lombok.val;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;

public class PendingBillSource implements DataSource<PendingBillRow> {

  private final BillingDataSource<InputPendingBillRow> pendingBillDataSource;
  private final BillingDataSource<InputPendingBillLineRow> pendingBillLineDataSource;
  private final BillingDataSource<InputPendingBillLineCalculationRow> pendingBillLineCalcDataSource;
  private final BillingDataSource<InputPendingBillLineServiceQuantityRow> pendingBillLineServiceQuantityDataSource;


  public PendingBillSource(PendingConfiguration pendingConfiguration) {
    pendingBillDataSource = new BillingDataSource<>("pending bill", "sql/inputs/get-pending-bills.sql",
        pendingConfiguration.getPendingBill(), InputEncodings.RAW_PENDING_BILL_ROW_ENCODER);
    pendingBillLineDataSource = new BillingDataSource<>("pending bill line", "sql/inputs/get-pending-bill-lines.sql",
        pendingConfiguration.getPendingBillLine(), InputEncodings.RAW_PENDING_BILL_LINE_ROW_ENCODER);
    pendingBillLineCalcDataSource = new BillingDataSource<>("pending bill line calc", "sql/inputs/get-pending-bill-line-calcs.sql",
        pendingConfiguration.getPendingBillLineCalc(), InputEncodings.RAW_PENDING_BILL_LINE_CALCULATION_ROW_ENCODER);
    pendingBillLineServiceQuantityDataSource = new BillingDataSource<>("pending bill line svc qty",
        "sql/inputs/get-pending-bill-line-svc-qtys.sql", pendingConfiguration.getPendingBillLineSvcQty(),
        InputEncodings.RAW_PENDING_BILL_LINE_SERVICE_QUANTITY_ROW_ENCODER);
  }

  @Override
  public Dataset<PendingBillRow> load(SparkSession sparkSession, BatchId batchId) {

    val pB = pendingBillDataSource.load(sparkSession, batchId);
    val pBL = pendingBillLineDataSource.load(sparkSession, batchId);
    val pBLC = pendingBillLineCalcDataSource.load(sparkSession, batchId);
    val pBLSQ = pendingBillLineServiceQuantityDataSource.load(sparkSession, batchId);

    val groupedPBLC = groupPendingBillLineCalcRowsByBillLineId(pBLC);
    val groupedPBLSQ = groupPendingBillLineSvcQtyRowsByBillLineId(pBLSQ);
    val groupedCalcAndSvcQty = joinCalcAndSvcQtyWithSameBillLineId(groupedPBLC, groupedPBLSQ);
    val groupedPBL = groupPendingBillLineByBillId(pBL, groupedCalcAndSvcQty);

    return createPendingBillsAndPopulateWithLines(pB, groupedPBL);
  }

  private Dataset<PendingBillRow> createPendingBillsAndPopulateWithLines(Dataset<InputPendingBillRow> pB,
      Dataset<Tuple2<String, PendingBillLineRow[]>> groupedPBL) {
    return pB
        .joinWith(groupedPBL, pB.col("billId").equalTo(groupedPBL.col(COL_NAME_VALUE)), JOIN_TYPE_INNER)
        .map(t -> PendingBillRow.from(t._1, t._2._2), Encodings.PENDING_BILL_ENCODER);
  }

  private Dataset<Tuple2<String, PendingBillLineRow[]>> groupPendingBillLineByBillId(Dataset<InputPendingBillLineRow> pBL,
      Dataset<Tuple3<String, PendingBillLineCalculationRow[], PendingBillLineServiceQuantityRow[]>> groupedCalcAndSvcQty) {

    return pBL
        .joinWith(groupedCalcAndSvcQty, pBL.col("billLineID").equalTo(groupedCalcAndSvcQty.col(COL_NAME_VALUE)), JOIN_TYPE_INNER)
        .map(t -> {
          InputPendingBillLineRow pendingBillLineRow = t._1;
          PendingBillLineCalculationRow[] pendingBillLineCalculations = t._2._2();
          PendingBillLineServiceQuantityRow[] pendingBillLineSvcQtys = t._2._3();

          return new Tuple3<>(pendingBillLineRow, pendingBillLineCalculations, pendingBillLineSvcQtys);
        }, Encoders.tuple(InputEncodings.RAW_PENDING_BILL_LINE_ROW_ENCODER, Encodings.PENDING_BILL_LINE_CALC_ROW_ARRAY_ENCODER,
            Encodings.PENDING_BILL_LINE_SVC_QTY_ROW_ARRAY_ENCODER))
        .groupByKey(tuple -> tuple._1().getBillId(), Encoders.STRING())
        .mapValues(tuple -> PendingBillLineRow.from(tuple._1(), tuple._2(), tuple._3()), Encodings.PENDING_BILL_LINE_ENCODER)
        .mapGroups(mapKeyAndPendingBillLineIteratorToTuple(), Encoders.tuple(Encoders.STRING(), Encodings.PENDING_BILL_LINE_ARRAY_ENCODER));
  }

  private Dataset<Tuple3<String, PendingBillLineCalculationRow[], PendingBillLineServiceQuantityRow[]>> joinCalcAndSvcQtyWithSameBillLineId(
      Dataset<Tuple2<String, PendingBillLineCalculationRow[]>> groupedPBLC,
      Dataset<Tuple2<String, PendingBillLineServiceQuantityRow[]>> groupedPBLSQ) {
    return groupedPBLC
        .joinWith(groupedPBLSQ, groupedPBLC.col(COL_NAME_VALUE).equalTo(groupedPBLSQ.col(COL_NAME_VALUE)), JOIN_TYPE_INNER)
        .map(t -> {
              String pendingBillLineId = t._1._1;
              PendingBillLineCalculationRow[] pendingBillLineCalculations = t._1._2;
              PendingBillLineServiceQuantityRow[] pendingBillLineSvcQtys = t._2._2;

              return new Tuple3<>(pendingBillLineId, pendingBillLineCalculations, pendingBillLineSvcQtys);
            },
            Encoders.tuple(Encoders.STRING(), Encodings.PENDING_BILL_LINE_CALC_ROW_ARRAY_ENCODER,
                Encodings.PENDING_BILL_LINE_SVC_QTY_ROW_ARRAY_ENCODER));
  }

  private Dataset<Tuple2<String, PendingBillLineServiceQuantityRow[]>> groupPendingBillLineSvcQtyRowsByBillLineId(
      Dataset<InputPendingBillLineServiceQuantityRow> pBLSQ) {
    return pBLSQ
        .groupByKey(InputPendingBillLineServiceQuantityRow::getBillLineId, Encoders.STRING())
        .mapValues(PendingBillLineServiceQuantityRow::from, Encodings.PENDING_BILL_LINE_SVC_QTY_ENCODER)
        .mapGroups(mapKeyAndPendingBillLineSvcQtyIteratorToTuple(),
            Encoders.tuple(Encoders.STRING(), Encodings.PENDING_BILL_LINE_SVC_QTY_ROW_ARRAY_ENCODER));
  }

  private Dataset<Tuple2<String, PendingBillLineCalculationRow[]>> groupPendingBillLineCalcRowsByBillLineId(
      Dataset<InputPendingBillLineCalculationRow> pBLC) {
    return pBLC
        .groupByKey(InputPendingBillLineCalculationRow::getBillLineId, Encoders.STRING())
        .mapValues(PendingBillLineCalculationRow::from, Encodings.PENDING_BILL_LINE_CALCULATION_ENCODER)
        .mapGroups(mapKeyAndPendingBillLineCalcIteratorToTuple(),
            Encoders.tuple(Encoders.STRING(), Encodings.PENDING_BILL_LINE_CALC_ROW_ARRAY_ENCODER));
  }

  private MapGroupsFunction<String, PendingBillLineCalculationRow, Tuple2<String, PendingBillLineCalculationRow[]>>
  mapKeyAndPendingBillLineCalcIteratorToTuple() {
    return (k, iter) -> new Tuple2<>(k, Iterators.toArray(iter, PendingBillLineCalculationRow.class));
  }

  private MapGroupsFunction<String, PendingBillLineServiceQuantityRow, Tuple2<String, PendingBillLineServiceQuantityRow[]>>
  mapKeyAndPendingBillLineSvcQtyIteratorToTuple() {
    return (k, iter) -> new Tuple2<>(k, Iterators.toArray(iter, PendingBillLineServiceQuantityRow.class));
  }

  private MapGroupsFunction<String, PendingBillLineRow, Tuple2<String, PendingBillLineRow[]>> mapKeyAndPendingBillLineIteratorToTuple() {
    return (k, iter) -> new Tuple2<>(k, Iterators.toArray(iter, PendingBillLineRow.class));
  }
}
