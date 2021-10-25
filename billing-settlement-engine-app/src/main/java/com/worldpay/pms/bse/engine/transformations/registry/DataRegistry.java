package com.worldpay.pms.bse.engine.transformations.registry;

import com.worldpay.pms.bse.engine.BillingConfiguration;
import com.worldpay.pms.bse.engine.BillingConfiguration.DataSources;
import com.worldpay.pms.bse.engine.BillingConfiguration.DataWriters;
import com.worldpay.pms.bse.engine.transformations.sources.BillCorrectionSource;
import com.worldpay.pms.bse.engine.transformations.sources.BillTaxSource;
import com.worldpay.pms.bse.engine.transformations.sources.MiscBillableItemSource;
import com.worldpay.pms.bse.engine.transformations.sources.PendingBillSource;
import com.worldpay.pms.bse.engine.transformations.sources.PendingMinChargeSource;
import com.worldpay.pms.bse.engine.transformations.sources.StandardBillableItemSource;
import com.worldpay.pms.bse.engine.transformations.writers.BillAccountingWriter;
import com.worldpay.pms.bse.engine.transformations.writers.BillErrorWriter;
import com.worldpay.pms.bse.engine.transformations.writers.BillLineDetailWriter;
import com.worldpay.pms.bse.engine.transformations.writers.BillPriceWriter;
import com.worldpay.pms.bse.engine.transformations.writers.BillRelationshipWriter;
import com.worldpay.pms.bse.engine.transformations.writers.BillTaxWriter;
import com.worldpay.pms.bse.engine.transformations.writers.BillWriter;
import com.worldpay.pms.bse.engine.transformations.writers.FailedBillableItemWriter;
import com.worldpay.pms.bse.engine.transformations.writers.FixedBillErrorWriter;
import com.worldpay.pms.bse.engine.transformations.writers.FixedFailedBillableItemWriter;
import com.worldpay.pms.bse.engine.transformations.writers.MinChargeBillsWriter;
import com.worldpay.pms.bse.engine.transformations.writers.PendingBillWriter;
import com.worldpay.pms.bse.engine.transformations.writers.PendingMinimumChargeWriter;
import io.vavr.control.Option;
import java.sql.Date;
import java.time.LocalDate;
import lombok.Value;
import org.apache.spark.sql.SparkSession;

@Value
public class DataRegistry {

  ReaderRegistry readerRegistry;
  WriterRegistry writerRegistry;

  public DataRegistry(BillingConfiguration billingConfiguration, SparkSession sparkSession, Option<Date> overrideAccruedDate,
      LocalDate logicalDate) {
    DataSources sources = billingConfiguration.getSources();
    DataWriters writers = billingConfiguration.getWriters();

    this.readerRegistry = new ReaderRegistry(
        new StandardBillableItemSource(sources.getBillableItems(), sources.getFailedBillableItems(), overrideAccruedDate,
            billingConfiguration.getMaxAttempts()),
        new MiscBillableItemSource(sources.getMiscBillableItems(), sources.getFailedBillableItems(), overrideAccruedDate,
            billingConfiguration.getMaxAttempts()),
        new PendingBillSource(sources.getPendingConfiguration()),
        new PendingMinChargeSource(sources.getPendingMinCharge(), sources.getMinCharge(), logicalDate),
        new BillCorrectionSource(sources.getBillCorrectionConfiguration()),
        new BillTaxSource(sources.getBillTax()));
    this.writerRegistry = new WriterRegistry(
        new PendingBillWriter(writers.getPendingBill()),
        new BillWriter(writers.getCompleteBill()),
        new BillLineDetailWriter(writers.getBillLineDetail()),
        new BillErrorWriter(writers.getBillError(), sparkSession),
        new FixedBillErrorWriter(writers.getBillError()),
        new FailedBillableItemWriter(writers.getFailedBillableItem(), sparkSession),
        new FixedFailedBillableItemWriter(writers.getFailedBillableItem()),
        new BillPriceWriter(writers.getBillPrice()),
        new BillTaxWriter(writers.getBillTax()),
        new PendingMinimumChargeWriter(writers.getPendingMinCharge()),
        new BillAccountingWriter(writers.getBillAccounting()),
        new BillRelationshipWriter(writers.getBillRelationship()),
        new MinChargeBillsWriter(writers.getMinChargeBills())
    );
  }
}
