package com.worldpay.pms.bse.engine;

import static com.worldpay.pms.bse.engine.Encodings.BILL_TAX;
import static org.apache.spark.sql.Encoders.bean;
import static org.apache.spark.sql.Encoders.kryo;
import static org.apache.spark.sql.Encoders.tuple;

import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillCorrectionRow;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillLineCalcCorrectionRow;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillLineCorrectionRow;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillLineSvcQtyCorrectionRow;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Encoder;
import scala.Tuple2;
import scala.Tuple3;

@UtilityClass
public class InputCorrectionEncodings {

  public static final Encoder<InputBillCorrectionRow> INPUT_BILL_CORRECTION_ROW_ENCODER = bean(InputBillCorrectionRow.class);
  public static final Encoder<InputBillLineCorrectionRow> INPUT_BILL_LINE_CORRECTION_ROW_ENCODER = bean(InputBillLineCorrectionRow.class);
  public static final Encoder<BillLineRow> BILL_LINE_CORRECTION_ROW_ENCODER = bean(BillLineRow.class);
  public static final Encoder<BillLineRow[]> BILL_LINE_CORRECTION_ROW_ARRAY_ENCODER = kryo(BillLineRow[].class);
  public static final Encoder<Tuple2<InputBillCorrectionRow, BillLineRow[]>> INPUT_BILL_ROW_WITH_LINES_ENCODER = tuple(
      INPUT_BILL_CORRECTION_ROW_ENCODER, BILL_LINE_CORRECTION_ROW_ARRAY_ENCODER);
  public static final Encoder<InputBillLineCalcCorrectionRow> INPUT_BILL_LINE_CALC_CORRECTION_ROW_ENCODER = bean(
      InputBillLineCalcCorrectionRow.class);
  public static final Encoder<BillLineCalculationRow> BILL_LINE_CALC_CORRECTION_ROW_ENCODER = bean(BillLineCalculationRow.class);
  public static final Encoder<BillLineCalculationRow[]> BILL_LINE_CALC_CORRECTION_ROW_ARRAY_ENCODER = kryo(BillLineCalculationRow[].class);
  public static final Encoder<InputBillLineSvcQtyCorrectionRow> INPUT_BILL_LINE_SVC_QTY_CORRECTION_ROW_ENCODER = bean(
      InputBillLineSvcQtyCorrectionRow.class);
  public static final Encoder<BillLineServiceQuantityRow> BILL_LINE_SVC_QTY_CORRECTION_ROW_ENCODER = bean(
      BillLineServiceQuantityRow.class);
  public static final Encoder<BillLineServiceQuantityRow[]> BILL_LINE_SVC_QTY_CORRECTION_ROW_ARRAY_ENCODER = kryo(
      BillLineServiceQuantityRow[].class);

  public static final Encoder<Tuple3<InputBillCorrectionRow, BillLineRow[], BillTax>> BILL_CORRECTION_TUPLE_ENCODER = tuple(
      INPUT_BILL_CORRECTION_ROW_ENCODER, BILL_LINE_CORRECTION_ROW_ARRAY_ENCODER, BILL_TAX);
}
