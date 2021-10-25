package com.worldpay.pms.bse.engine.transformations;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.worldpay.pms.bse.domain.account.AccountRepository;
import com.worldpay.pms.bse.domain.model.BillableItem;
import com.worldpay.pms.bse.engine.BillingConfiguration.AccountRepositoryConfiguration;
import com.worldpay.pms.bse.engine.BillingConfiguration.Billing;
import com.worldpay.pms.bse.engine.data.AccountRepositoryFactory;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemRow;
import com.worldpay.pms.spark.core.factory.Factory;
import java.sql.Date;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

class BillableItemProcessingServiceFactoryTest {

  @Test
  void dummyTest() {
    Factory<AccountRepository> accountRepositoryFactory = new AccountRepositoryFactory(new AccountRepositoryConfiguration(),
        LocalDate.of(2021, 1, 10));
    BillableItemProcessingServiceFactory billableItemProcessingServiceFactory =
        new BillableItemProcessingServiceFactory(new AccountDeterminationServiceFactory(accountRepositoryFactory, new Billing()
            ));
    assertThrows(NullPointerException.class, () -> billableItemProcessingServiceFactory.build().get().process(getTestBillableItem()));
  }

  private BillableItem getTestBillableItem() {
    return BillableItemRow.builder()
        .billingCurrency("EUR")
        .currencyFromScheme("GBP")
        .priceCurrency("GBP")
        .fundingCurrency("EUR")
        .accruedDate(Date.valueOf(LocalDate.now()))
        .productId("MOVRPAY")
        .priceLineId("124")
        .legalCounterparty("00001")
        .granularity("gran")
        .granularityKeyValue("gran|kv")
        .settlementLevelType("SLT")
        .adhocBillFlag("Y")
        .aggregationHash("1245")
        .subAccountId("123456")
        .build();
  }
}