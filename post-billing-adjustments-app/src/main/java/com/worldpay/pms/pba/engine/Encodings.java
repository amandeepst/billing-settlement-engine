package com.worldpay.pms.pba.engine;

import static org.apache.spark.sql.Encoders.STRING;
import static org.apache.spark.sql.Encoders.bean;
import static org.apache.spark.sql.Encoders.kryo;
import static org.apache.spark.sql.Encoders.tuple;

import com.worldpay.pms.pba.domain.model.BillAdjustment;
import com.worldpay.pms.pba.engine.model.input.BillRow;
import com.worldpay.pms.pba.engine.model.output.BillAdjustmentAccountingRow;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Encoder;
import scala.Tuple2;

@UtilityClass
public class Encodings {

  public static final Encoder<BillRow> BILL_ROW_ENCODER = bean(BillRow.class);
  public static final Encoder<BillAdjustment> BILL_ADJUSTMENT_ENCODER = kryo(BillAdjustment.class);
  public static final Encoder<Tuple2<String, BillAdjustment>> BILL_ADJUSTMENT_WITH_ID_ENCODER = tuple(STRING(), BILL_ADJUSTMENT_ENCODER);

  public static final Encoder<BillRow[]> BILL_ROW_ARRAY_ENCODER = kryo(BillRow[].class);

  public static final Encoder<BillAdjustmentAccountingRow> BILL_ADJUSTMENT_ACCOUNTING_ENCODER = kryo(BillAdjustmentAccountingRow.class);

}
