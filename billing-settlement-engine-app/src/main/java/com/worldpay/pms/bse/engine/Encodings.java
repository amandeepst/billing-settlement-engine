package com.worldpay.pms.bse.engine;

import static org.apache.spark.sql.Encoders.STRING;
import static org.apache.spark.sql.Encoders.bean;
import static org.apache.spark.sql.Encoders.kryo;
import static org.apache.spark.sql.Encoders.tuple;

import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTaxDetail;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumChargeKey;
import com.worldpay.pms.bse.domain.model.mincharge.PendingMinimumCharge;
import com.worldpay.pms.bse.engine.transformations.CompleteBillResult;
import com.worldpay.pms.bse.engine.transformations.PendingBillResult;
import com.worldpay.pms.bse.engine.transformations.UpdateBillResult;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemLineRow;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemRow;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.billaccounting.BillAccountingRow;
import com.worldpay.pms.bse.engine.transformations.model.billerror.BillError;
import com.worldpay.pms.bse.engine.transformations.model.billprice.BillPriceRow;
import com.worldpay.pms.bse.engine.transformations.model.billrelationship.BillRelationshipRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetailRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import com.worldpay.pms.bse.engine.transformations.model.failedbillableitem.FailedBillableItem;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.MinimumChargeBillRow;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.PendingMinChargeRow;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.PendingMinChargeRowWithKey;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.aggregation.PendingBillAggKey;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Encoder;
import scala.Tuple2;

@UtilityClass
public class Encodings {

  public static final Encoder<BillableItemRow> BILLABLE_ITEM_ROW_ENCODER = bean(BillableItemRow.class);
  public static final Encoder<BillableItemLineRow> BILLABLE_ITEM_LINE_ROW_ENCODER = bean(BillableItemLineRow.class);
  public static final Encoder<BillableItemServiceQuantityRow> BILLABLE_ITEM_SERVICE_QUANTITY_ROW_ENCODER =
      bean(BillableItemServiceQuantityRow.class);

  public static final Encoder<PendingBillRow> PENDING_BILL_ENCODER = bean(PendingBillRow.class);
  public static final Encoder<PendingBillLineRow> PENDING_BILL_LINE_ENCODER = bean(PendingBillLineRow.class);
  public static final Encoder<PendingBillLineCalculationRow> PENDING_BILL_LINE_CALCULATION_ENCODER = bean(
      PendingBillLineCalculationRow.class);
  public static final Encoder<PendingBillLineServiceQuantityRow> PENDING_BILL_LINE_SVC_QTY_ENCODER = bean(
      PendingBillLineServiceQuantityRow.class);

  public static final Encoder<PendingBillAggKey> PENDING_BILL_AGG_KEY_ENCODER = kryo(PendingBillAggKey.class);

  public static final Encoder<PendingBillLineCalculationRow[]> PENDING_BILL_LINE_CALC_ROW_ARRAY_ENCODER = kryo(
      PendingBillLineCalculationRow[].class);
  public static final Encoder<PendingBillLineServiceQuantityRow[]> PENDING_BILL_LINE_SVC_QTY_ROW_ARRAY_ENCODER = kryo(
      PendingBillLineServiceQuantityRow[].class);
  public static final Encoder<PendingBillLineRow[]> PENDING_BILL_LINE_ARRAY_ENCODER = kryo(PendingBillLineRow[].class);

  public static final Encoder<PendingBillResult> PENDING_BILL_RESULT_ENCODER = kryo(PendingBillResult.class);
  public static final Encoder<FailedBillableItem> FAILED_BILLABLE_ITEM_ENCODER = kryo(FailedBillableItem.class);

  public static final Encoder<BillAccountingRow> BILL_ACCOUNTING_ROW_ENCODER = bean(BillAccountingRow.class);

  public static final Encoder<BillRow> BILL_ROW_ENCODER = bean(BillRow.class);
  public static final Encoder<BillLineDetailRow> BILL_LINE_DETAIL_ROW_ENCODER = kryo(BillLineDetailRow.class);
  public static final Encoder<BillPriceRow> BILL_PRICE_ROW_ENCODER = bean(BillPriceRow.class);
  public static final Encoder<Tuple2<String, BillPriceRow>> BILL_PRICE_WITH_ID = tuple(STRING(), BILL_PRICE_ROW_ENCODER);

  public static final Encoder<BillError> BILL_ERROR_ENCODER = kryo(BillError.class);
  public static final Encoder<CompleteBillResult> COMPLETE_BILL_RESULT_ENCODER = kryo(CompleteBillResult.class);
  public static final Encoder<UpdateBillResult> UPDATE_BILL_RESULT_ENCODER = kryo(UpdateBillResult.class);
  public static final Encoder<BillTax> BILL_TAX = bean(BillTax.class);
  public static final Encoder<BillTaxDetail> BILL_TAX_DETAIL = bean(BillTaxDetail .class);
  public static final Encoder<BillTaxDetail[]> BILL_TAX_DETAIL_ARRAY = kryo(BillTaxDetail[].class);
  public static final Encoder<BillRelationshipRow> BILL_RELATIONSHIP_ENCODER = kryo(BillRelationshipRow.class);

  public static final Encoder<PendingMinimumCharge> PENDING_MINIMUM_CHARGE_ENCODER = kryo(PendingMinimumCharge.class);
  public static final Encoder<PendingMinChargeRow> PENDING_MINIMUM_CHARGE_ROW_ENCODER = bean(PendingMinChargeRow.class);
  public static final Encoder<MinimumChargeBillRow> MINIMUM_CHARGE_BILL_ROW_ENCODER = bean(MinimumChargeBillRow.class);
  public static final Encoder<PendingMinChargeRow[]> PENDING_MINIMUM_CHARGE_ARRAY_ENCODER = kryo(PendingMinChargeRow[].class);

  public static final Encoder<Tuple2<PendingBillRow, PendingMinChargeRow[]>> PENDING_BILL_MINIMUM_CHARGE_TUPLE_ENCODER =
      tuple(PENDING_BILL_ENCODER, PENDING_MINIMUM_CHARGE_ARRAY_ENCODER);

  public static final Encoder<MinimumChargeKey> MINIMUM_CHARGE_KEY_ENCODER = kryo(MinimumChargeKey.class);

  public static final Encoder<PendingMinChargeRowWithKey> PENDING_MINIMUM_CHARGE_TUPLE_ENCODER = bean(PendingMinChargeRowWithKey.class);
}
