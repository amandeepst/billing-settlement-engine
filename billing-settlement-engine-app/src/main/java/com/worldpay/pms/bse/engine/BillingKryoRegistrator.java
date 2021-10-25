package com.worldpay.pms.bse.engine;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsEmptyMapSerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonListSerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonMapSerializer;
import com.worldpay.pms.bse.domain.BillableItemProcessingService;
import com.worldpay.pms.bse.domain.DefaultBillableItemProcessingService;
import com.worldpay.pms.bse.domain.account.AccountDeterminationService;
import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountDetails;
import com.worldpay.pms.bse.domain.account.BillingAccount;
import com.worldpay.pms.bse.domain.account.BillingCycle;
import com.worldpay.pms.bse.domain.account.DefaultAccountDeterminationService;
import com.worldpay.pms.bse.domain.model.billtax.BillLineTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.billtax.BillTaxDetail;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumCharge;
import com.worldpay.pms.bse.domain.model.mincharge.PendingMinimumCharge;
import com.worldpay.pms.bse.domain.model.tax.Party;
import com.worldpay.pms.bse.domain.model.tax.ProductCharacteristic;
import com.worldpay.pms.bse.domain.model.tax.TaxRate;
import com.worldpay.pms.bse.domain.model.tax.TaxRule;
import com.worldpay.pms.bse.domain.validation.DefaultValidationService;
import com.worldpay.pms.bse.domain.validation.ValidationResult;
import com.worldpay.pms.bse.domain.validation.ValidationService;
import com.worldpay.pms.bse.engine.BillingConfiguration.BillIdSeqConfiguration;
import com.worldpay.pms.bse.engine.data.AccountRepositoryFactory;
import com.worldpay.pms.bse.engine.data.BillingRepositoryFactory;
import com.worldpay.pms.bse.engine.data.CsvStaticDataRepository;
import com.worldpay.pms.bse.engine.data.DefaultBillingRepository;
import com.worldpay.pms.bse.engine.data.billdisplay.CalculationTypeBillDisplay;
import com.worldpay.pms.bse.engine.domain.billcorrection.BillCorrectionService;
import com.worldpay.pms.bse.engine.domain.billcorrection.BillCorrectionServiceFactory;
import com.worldpay.pms.bse.engine.transformations.AccountDeterminationServiceFactory;
import com.worldpay.pms.bse.engine.transformations.BillIdGeneratorFactory;
import com.worldpay.pms.bse.engine.transformations.BillProcessingServiceFactory;
import com.worldpay.pms.bse.engine.transformations.BillableItemProcessingServiceFactory;
import com.worldpay.pms.bse.engine.transformations.CompleteBillResult;
import com.worldpay.pms.bse.engine.transformations.PendingBillResult;
import com.worldpay.pms.bse.engine.transformations.UpdateBillResult;
import com.worldpay.pms.bse.engine.transformations.model.PendingOrCompleteBill;
import com.worldpay.pms.bse.engine.transformations.model.billerror.BillError;
import com.worldpay.pms.bse.engine.transformations.model.billerror.BillErrorDetail;
import com.worldpay.pms.bse.engine.transformations.model.billprice.BillPriceRow;
import com.worldpay.pms.bse.engine.transformations.model.billrelationship.BillRelationshipRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetail;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetailRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import com.worldpay.pms.bse.engine.transformations.model.failedbillableitem.FailedBillableItem;
import com.worldpay.pms.bse.engine.transformations.model.input.misc.MiscBillableItemLineRow;
import com.worldpay.pms.bse.engine.transformations.model.input.standard.StandardBillableItemLineRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.aggregation.PendingBillAggKey;
import com.worldpay.pms.spark.KryoConfiguration;
import com.worldpay.pms.spark.core.DataSource;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import java.util.Collections;

public class BillingKryoRegistrator extends KryoConfiguration {

  @Override
  public void registerClasses(Kryo kryo) {
    super.registerClasses(kryo);

    // datasource
    kryo.register(DataSource.class);
    kryo.register(JdbcConfiguration.class);

    kryo.register(StandardBillableItemLineRow.class);
    kryo.register(StandardBillableItemLineRow[].class);

    kryo.register(PendingBillRow.class);
    kryo.register(PendingBillRow[].class);
    kryo.register(PendingBillLineRow.class);
    kryo.register(PendingBillLineRow[].class);
    kryo.register(PendingBillLineCalculationRow.class);
    kryo.register(PendingBillLineCalculationRow[].class);
    kryo.register(PendingBillLineServiceQuantityRow.class);
    kryo.register(PendingBillLineServiceQuantityRow[].class);
    kryo.register(BillLineDetail.class);
    kryo.register(BillLineDetail[].class);

    kryo.register(PendingBillAggKey.class);

    kryo.register(MiscBillableItemLineRow.class);
    kryo.register(MiscBillableItemLineRow[].class);

    kryo.register(BillRow.class);
    kryo.register(BillLineRow.class);
    kryo.register(BillLineRow[].class);
    kryo.register(BillLineServiceQuantityRow.class);
    kryo.register(BillLineServiceQuantityRow[].class);
    kryo.register(BillLineCalculationRow.class);
    kryo.register(BillLineCalculationRow[].class);

    kryo.register(java.math.BigDecimal.class);
    kryo.register(java.util.HashMap.class);
    kryo.register(java.time.LocalDate.class);
    kryo.register(Collections.singletonMap("", "").getClass(), new CollectionsSingletonMapSerializer());
    kryo.register(Collections.singletonList("").getClass(), new CollectionsSingletonListSerializer());
    kryo.register(Collections.emptyMap().getClass(), new CollectionsEmptyMapSerializer());
    kryo.register(scala.collection.convert.Wrappers$.class);
    kryo.register(org.apache.spark.unsafe.types.UTF8String.class);
    kryo.register(io.vavr.collection.HashMap.class);
    kryo.register(org.apache.spark.sql.types.Decimal.class);
    kryo.register(scala.math.BigDecimal.class);
    kryo.register(java.math.MathContext.class);
    kryo.register(java.math.RoundingMode.class);

    kryo.register(ValidationService.class);
    kryo.register(DefaultValidationService.class);
    kryo.register(AccountDeterminationService.class);
    kryo.register(DefaultAccountDeterminationService.class);
    kryo.register(BillableItemProcessingService.class);
    kryo.register(DefaultBillableItemProcessingService.class);
    kryo.register(BillingCycle.class);
    kryo.register(BillingAccount.class);
    kryo.register(ValidationResult.class);

    kryo.register(PendingBillResult.class);
    kryo.register(FailedBillableItem.class);
    kryo.register(DefaultBillingRepository.class);
    kryo.register(CsvStaticDataRepository.class);
    kryo.register(BillableItemProcessingServiceFactory.class);
    kryo.register(BillingRepositoryFactory.class);
    kryo.register(CalculationTypeBillDisplay.class);
    kryo.register(AccountRepositoryFactory.class);
    kryo.register(BillProcessingServiceFactory.class);
    kryo.register(AccountDeterminationServiceFactory.class);
    kryo.register(BillCorrectionService.class);
    kryo.register(BillCorrectionServiceFactory.class);

    kryo.register(BillingConfiguration.Billing.class);
    kryo.register(BillingConfiguration.AccountRepositoryConfiguration.class);
    kryo.register(BillingConfiguration.StaticDataRepositoryConfiguration.class);

    kryo.register(BillError.class);
    kryo.register(BillErrorDetail.class);
    kryo.register(BillErrorDetail[].class);

    kryo.register(PendingOrCompleteBill.class);
    kryo.register(CompleteBillResult.class);
    kryo.register(PendingMinimumCharge.class);
    kryo.register(PendingMinimumCharge[].class);
    kryo.register(MinimumCharge.class);

    kryo.register(BillPriceRow.class);
    kryo.register(BillPriceRow[].class);
    kryo.register(BillLineDetailRow[].class);
    kryo.register(BillLineDetailRow.class);
    kryo.register(AccountDetails.class);

    kryo.register(UpdateBillResult.class);
    kryo.register(BillTax.class);
    kryo.register(BillTaxDetail.class);
    kryo.register(BillTaxDetail[].class);
    kryo.register(BillLineTax.class);

    kryo.register(TaxRate.class);
    kryo.register(TaxRule.class);
    kryo.register(Party.class);
    kryo.register(ProductCharacteristic.class);
    kryo.register(BillRelationshipRow.class);

    kryo.register(com.worldpay.pms.bse.domain.model.mincharge.MinimumChargeKey.class);
    kryo.register(com.worldpay.pms.bse.engine.transformations.model.input.mincharge.PendingMinChargeRow.class);
    kryo.register(com.worldpay.pms.bse.engine.transformations.model.input.mincharge.PendingMinChargeRow[].class);

    kryo.register(BillIdGeneratorFactory.class);
    kryo.register(BillIdSeqConfiguration.class);
  }
}
