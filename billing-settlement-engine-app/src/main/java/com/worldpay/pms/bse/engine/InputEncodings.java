package com.worldpay.pms.bse.engine;

import static org.apache.spark.sql.Encoders.bean;
import static org.apache.spark.sql.Encoders.kryo;
import static org.apache.spark.sql.Encoders.tuple;

import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.input.billtax.InputBillTaxDetailRow;
import com.worldpay.pms.bse.engine.transformations.model.input.billtax.InputBillTaxRow;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillCorrectionRow;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.MinimumChargeBillRow;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.PendingMinChargeRow;
import com.worldpay.pms.bse.engine.transformations.model.input.misc.MiscBillableItemLineRow;
import com.worldpay.pms.bse.engine.transformations.model.input.misc.MiscBillableItemRow;
import com.worldpay.pms.bse.engine.transformations.model.input.pending.InputPendingBillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.input.pending.InputPendingBillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.input.pending.InputPendingBillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.input.pending.InputPendingBillRow;
import com.worldpay.pms.bse.engine.transformations.model.input.standard.StandardBillableItemLineRow;
import com.worldpay.pms.bse.engine.transformations.model.input.standard.StandardBillableItemRow;
import com.worldpay.pms.bse.engine.transformations.model.input.standard.StandardBillableItemSvcQtyRow;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Encoder;
import scala.Tuple2;

@UtilityClass
public class InputEncodings {

  public static final Encoder<StandardBillableItemRow> STANDARD_BILLABLE_ITEM_ROW_ENCODER = bean(StandardBillableItemRow.class);
  public static final Encoder<StandardBillableItemLineRow> STANDARD_BILLABLE_ITEM_LINE_ROW_ENCODER =
      bean(StandardBillableItemLineRow.class);
  public static final Encoder<StandardBillableItemSvcQtyRow> STANDARD_BILLABLE_ITEM_SVC_QTY_ROW_ENCODER =
      bean(StandardBillableItemSvcQtyRow.class);
  public static final Encoder<StandardBillableItemLineRow[]> STANDARD_BILLABLE_ITEM_LINE_ROW_ARRAY_ENCODER =
      kryo(StandardBillableItemLineRow[].class);

  public static final Encoder<MiscBillableItemRow> MISC_BILLABLE_ITEM_ROW_ENCODER = bean(MiscBillableItemRow.class);
  public static final Encoder<MiscBillableItemLineRow> MISC_BILLABLE_ITEM_LINE_ROW_ENCODER = bean(MiscBillableItemLineRow.class);
  public static final Encoder<MiscBillableItemLineRow[]> MISC_BILLABLE_ITEM_LINE_ROW_ARRAY_ENCODER = kryo(MiscBillableItemLineRow[].class);

  public static final Encoder<InputPendingBillRow> RAW_PENDING_BILL_ROW_ENCODER = bean(InputPendingBillRow.class);
  public static final Encoder<InputPendingBillLineRow> RAW_PENDING_BILL_LINE_ROW_ENCODER = bean(InputPendingBillLineRow.class);
  public static final Encoder<InputPendingBillLineCalculationRow> RAW_PENDING_BILL_LINE_CALCULATION_ROW_ENCODER = bean(
      InputPendingBillLineCalculationRow.class);
  public static final Encoder<InputPendingBillLineServiceQuantityRow> RAW_PENDING_BILL_LINE_SERVICE_QUANTITY_ROW_ENCODER = bean(
      InputPendingBillLineServiceQuantityRow.class);

  public static final Encoder<PendingMinChargeRow> PENDING_MIN_CHARGE_ROW_ENCODER = bean(PendingMinChargeRow.class);
  public static final Encoder<MinimumChargeBillRow> MIN_CHARGE_BILL_ROW_ENCODER = bean(MinimumChargeBillRow.class);

  public static final Encoder<InputBillTaxRow> INPUT_BILL_TAX_ROW = bean(InputBillTaxRow.class);
  public static final Encoder<InputBillTaxDetailRow> INPUT_BILL_TAX_DETAIL_ROW = bean(InputBillTaxDetailRow.class);

  public static final Encoder<Tuple2<InputBillCorrectionRow, BillLineRow[]>> BILL_CORRECTION_ROW_ENCODER = tuple(
      bean(InputBillCorrectionRow.class), kryo(BillLineRow[].class));

}
