package com.worldpay.pms.bse.engine.transformations;

import com.worldpay.pms.bse.domain.BillProcessingService;
import com.worldpay.pms.bse.domain.DefaultBillProcessingService;
import com.worldpay.pms.bse.domain.DefaultRoundingService;
import com.worldpay.pms.bse.domain.RoundingService;
import com.worldpay.pms.bse.domain.account.AccountDeterminationService;
import com.worldpay.pms.bse.domain.mincharge.DefaultMinimumChargeCalculationService;
import com.worldpay.pms.bse.domain.mincharge.MinimumChargeCalculationService;
import com.worldpay.pms.bse.domain.mincharge.PreloadedMinimumChargeStore;
import com.worldpay.pms.bse.domain.tax.DefaultTaxCalculationService;
import com.worldpay.pms.bse.domain.tax.DefaultTaxRateDeterminationService;
import com.worldpay.pms.bse.domain.tax.DefaultTaxRuleDeterminationService;
import com.worldpay.pms.bse.domain.tax.TaxCalculationService;
import com.worldpay.pms.bse.engine.BillingConfiguration.Billing;
import com.worldpay.pms.bse.engine.data.BillingRepository;
import com.worldpay.pms.spark.core.factory.Factory;
import io.vavr.control.Option;
import io.vavr.control.Try;
import java.time.LocalDate;
import java.util.List;

public class BillProcessingServiceFactory implements Factory<BillProcessingService> {

  private final Factory<AccountDeterminationService> accountDeterminationServiceFactory;
  private final Factory<BillingRepository> billingRepositoryFactory;
  private final LocalDate logicalDate;
  private final List<String> processingGroups;
  private final List<String> billingTypes;
  private final Billing billingConfig;

  public BillProcessingServiceFactory(Factory<AccountDeterminationService> accountDeterminationServiceFactory,
      Factory<BillingRepository> billingRepositoryFactory, LocalDate logicalDate, List<String> processingGroups,
      List<String> billingTypes, Billing billingConfig) {
    this.accountDeterminationServiceFactory = accountDeterminationServiceFactory;
    this.billingRepositoryFactory = billingRepositoryFactory;
    this.logicalDate = logicalDate;
    this.processingGroups = processingGroups;
    this.billingTypes = billingTypes;
    this.billingConfig = billingConfig;
  }

  @Override
  public Try<BillProcessingService> build() {
    BillingRepository billingRepository = billingRepositoryFactory.build().get();
    RoundingService roundingService = new DefaultRoundingService(billingRepository.getCurrencies());
    TaxCalculationService taxCalculationService = new DefaultTaxCalculationService(
        new DefaultTaxRuleDeterminationService(billingRepository.getTaxRules(), billingRepository.getParties()),
        new DefaultTaxRateDeterminationService(billingRepository.getProductCharacteristics(), billingRepository.getTaxRates()),
        billingRepository.getParties(),
        roundingService
    );
    PreloadedMinimumChargeStore minimumChargeStore = new PreloadedMinimumChargeStore(
        billingConfig.getMinChargeListMaxEntries(),
        billingConfig.getMinChargeListConcurrency(),
        billingConfig.getMinChargeListMaxEntries(),
        minimumChargeKey -> Option.none());

    minimumChargeStore.preloadMinCharge(billingRepository.getMinimumCharges());
    MinimumChargeCalculationService minimumChargeCalculationService = new DefaultMinimumChargeCalculationService(
        logicalDate,
        minimumChargeStore,
        roundingService);

    return Try.of(() -> new DefaultBillProcessingService(
        logicalDate,
        processingGroups,
        billingTypes,
        accountDeterminationServiceFactory.build().get(),
        minimumChargeCalculationService,
        taxCalculationService,
        roundingService
    ));
  }
}