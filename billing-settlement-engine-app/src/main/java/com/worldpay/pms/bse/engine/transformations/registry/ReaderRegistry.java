package com.worldpay.pms.bse.engine.transformations.registry;

import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillCorrectionRow;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.PendingMinChargeRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import com.worldpay.pms.spark.core.DataSource;
import lombok.Value;
import scala.Tuple2;

@Value
public class ReaderRegistry {

  DataSource<BillableItemRow> standardBillableItemSource;
  DataSource<BillableItemRow> miscBillableItemSource;
  DataSource<PendingBillRow> pendingBillSource;
  DataSource<PendingMinChargeRow> pendingMinChargeSource;
  DataSource<Tuple2<InputBillCorrectionRow, BillLineRow[]>> billCorrectionSource;
  DataSource<BillTax> billTaxDataSource;
}
