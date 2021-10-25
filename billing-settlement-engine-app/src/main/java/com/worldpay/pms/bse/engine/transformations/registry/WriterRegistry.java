package com.worldpay.pms.bse.engine.transformations.registry;

import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.engine.transformations.model.billaccounting.BillAccountingRow;
import com.worldpay.pms.bse.engine.transformations.model.billerror.BillError;
import com.worldpay.pms.bse.engine.transformations.model.billprice.BillPriceRow;
import com.worldpay.pms.bse.engine.transformations.model.billrelationship.BillRelationshipRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetailRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import com.worldpay.pms.bse.engine.transformations.model.failedbillableitem.FailedBillableItem;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.MinimumChargeBillRow;
import com.worldpay.pms.bse.engine.transformations.model.input.mincharge.PendingMinChargeRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import com.worldpay.pms.spark.core.DataWriter;
import com.worldpay.pms.spark.core.ErrorWriter;
import lombok.Value;
import scala.Tuple2;

@Value
public class WriterRegistry {

  DataWriter<PendingBillRow> pendingBillWriter;
  DataWriter<BillRow> completeBillWriter;
  DataWriter<BillLineDetailRow> billLineDetailWriter;
  ErrorWriter<BillError> billErrorWriter;
  DataWriter<BillError> fixedBillErrorWriter;
  ErrorWriter<FailedBillableItem> failedBillableItemWriter;
  DataWriter<FailedBillableItem> fixedBillableWriter;
  DataWriter<Tuple2<String, BillPriceRow>> billPriceWriter;
  DataWriter<BillTax> billTaxWriter;
  DataWriter<PendingMinChargeRow> pendingMinChargeWriter;
  DataWriter<BillAccountingRow> billAccountingWriter;
  DataWriter<BillRelationshipRow> billRelationshipWriter;
  DataWriter<MinimumChargeBillRow> minimumChargeBillWriter;
}
