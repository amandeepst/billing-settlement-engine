package com.worldpay.pms.bse.engine.transformations;

import com.worldpay.pms.bse.domain.BillableItemProcessingService;
import com.worldpay.pms.bse.domain.DefaultBillableItemProcessingService;
import com.worldpay.pms.bse.domain.account.AccountDeterminationService;
import com.worldpay.pms.bse.domain.validation.DefaultValidationService;
import com.worldpay.pms.bse.domain.validation.DefaultCalculationCheckService;
import com.worldpay.pms.spark.core.factory.Factory;
import io.vavr.control.Try;

public class BillableItemProcessingServiceFactory implements Factory<BillableItemProcessingService> {

  private final Factory<AccountDeterminationService> accountDeterminationServiceFactory;

  public BillableItemProcessingServiceFactory(
      Factory<AccountDeterminationService> accountDeterminationServiceFactory) {
    this.accountDeterminationServiceFactory = accountDeterminationServiceFactory;
  }

  @Override
  public Try<BillableItemProcessingService> build() {
    return Try.of(() -> new DefaultBillableItemProcessingService(
        new DefaultValidationService(),
        new DefaultCalculationCheckService(),
        accountDeterminationServiceFactory.build().get()));
  }
}