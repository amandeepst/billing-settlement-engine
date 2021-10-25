package com.worldpay.pms.bse.engine.data;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;

import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountDetails;
import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountKey;
import com.worldpay.pms.bse.domain.account.AccountRepository;
import com.worldpay.pms.bse.domain.account.BillingAccount;
import com.worldpay.pms.bse.domain.account.BillingCycle;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumCharge;
import com.worldpay.pms.bse.domain.model.tax.Party;
import com.worldpay.pms.bse.domain.model.tax.ProductCharacteristic;
import com.worldpay.pms.bse.domain.staticdata.CalculationLineTypeDescription;
import com.worldpay.pms.bse.domain.staticdata.Currency;
import com.worldpay.pms.bse.domain.staticdata.ProductDescription;
import com.worldpay.pms.bse.domain.staticdata.ServiceQuantityDescription;
import com.worldpay.pms.bse.engine.utils.WithDatabase;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import io.vavr.collection.List;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BillingRepositoryTest implements WithDatabase {

  private JdbcConfiguration staticDataRepositoryConfig;

  private DefaultBillingRepository billingRepository;

  public void bindBillingJdbcConfiguration(JdbcConfiguration conf) {
    staticDataRepositoryConfig = conf;
  }

  @BeforeEach
  void setUp() {
    billingRepository = new DefaultBillingRepository(Date.valueOf("2021-03-20"), new TestJdbcStaticDataRepository(staticDataRepositoryConfig),
        new CsvStaticDataRepository(Date.valueOf(LocalDate.now())),
        new TestJdbcAccountRepository());
  }

  @Test
  void whenGetPartiesThenReturnCorrectResult() {
    Iterable<Party> parties = billingRepository.getParties();
    assertThat(parties, iterableWithSize(3));
    assertThat(parties, hasItem(party("PO4009970241", "ZZZ", null, "GB123456789", "Y")));
    assertThat(parties, hasItem(party("PO4009970242", "GBR", "NON-EU", "GB123456789", "Y")));
    assertThat(parties, hasItem(party("PO4009970243", "GBR", "NON-EU", null, "N")));
  }

  @Test
  void whenGetProductCharacteristicsThenReturnListOfProductCharacteristics() {
    Iterable<ProductCharacteristic> productCharacteristics = billingRepository.getProductCharacteristics();
    assertThat(productCharacteristics, iterableWithSize(2));
    assertThat(productCharacteristics, hasItem(productCharacteristic("PRMCCC06", "TX_S_ROI", "EXEMPT-E")));
    assertThat(productCharacteristics, hasItem(productCharacteristic("PPMCDC05", "CR_DR", "DR")));
  }

  @Test
  void whenGetRoundingScaleThenReturnCorrectResult() {
    assertThat(billingRepository.getRoundingScale("ABC"), is((short)3));
    assertThat(billingRepository.getRoundingScale("ZZZ"), is((short)2));
  }

  @Test
  void whenGetMinimumChargeCorrectResult() {
    Iterable<MinimumCharge> minimumCharges = billingRepository.getMinimumCharges();
    assertThat(minimumCharges, iterableWithSize(1));
    assertThat(minimumCharges,
        hasItem(new MinimumCharge("1595655016", "PO1100000001", "PO4000018151", "MIN_P_CHRG", BigDecimal.valueOf(15L), "GBP",
            new BillingCycle("WPMO", LocalDate.parse("2021-03-01"), LocalDate.parse("2021-03-31")))));
  }

  private static class TestJdbcStaticDataRepository extends JdbcStaticDataRepository{

    public TestJdbcStaticDataRepository(JdbcConfiguration conf) {
      super(conf, Date.valueOf(LocalDate.now()));
    }

    @Override
    public Iterable<ProductCharacteristic> getProductCharacteristics() {
      return List.of(productCharacteristic("PRMCCC06", "TX_S_ROI", "EXEMPT-E"),
          productCharacteristic("PPMCDC05", "CR_DR", "DR"));
    }

    @Override
    public Iterable<Currency> getDatabaseCurrencies() {
      return List.of(new Currency("GPB", (short) 2),
          new Currency("ABC", (short) 3));
    }

    @Override
    public Iterable<ProductDescription> getDatabaseProductDescription() {
      return List.empty();
    }

    @Override
    public Iterable<ServiceQuantityDescription> getDatabaseServiceQuantityDescription() {
      return List.empty();
    }

    @Override
    public Iterable<CalculationLineTypeDescription> getDatabaseCalculationLineTypeDescriptions() {
      return List.empty();
    }

    @Override
    public Iterable<MinimumCharge> getMinimumCharge() {
      return List.of(new MinimumCharge("1595655016", "PO1100000001", "PO4000018151", "MIN_P_CHRG", BigDecimal.valueOf(15L), "GBP",
          new BillingCycle("WPMO", null, null)));
    }
  }

  private static class TestJdbcAccountRepository implements AccountRepository {

    @Override
    public Iterable<BillingAccount> getAccountBySubAccountId(AccountKey key) {
      return List.empty();
    }

    @Override
    public Iterable<BillingCycle> getBillingCycles() {
      return List.of(new BillingCycle("WPMO", LocalDate.parse("2021-03-01"), LocalDate.parse("2021-03-31")),
          new BillingCycle("WPMO", LocalDate.parse("2021-01-01"), LocalDate.parse("2021-01-31"))
      );
    }

    @Override
    public Iterable<AccountDetails> getAccountDetailsByAccountId(String accountId) {
      return List.empty();
    }

    @Override
    public Iterable<Party> getParties() {
      return List.of(party("PO4009970241", "ZZZ", null, "GB123456789", null),
          party("PO4009970242", "GBR", null, "GB123456789", null),
          party("PO4009970243", "GBR", null, null, null));
    }

    @Override
    public Iterable<BillingAccount> getChargingAccountByPartyId(String partyId, String legalCounterParty, String currency) {
      return null;
    }
  }

  private static Party party(String partyId,
      String countryCode,
      String merchantRegion,
      String merchantTaxRegistration,
      String merchantTaxRegistered) {
    return new Party(partyId, countryCode, merchantRegion, merchantTaxRegistration, merchantTaxRegistered);
  }

  private static ProductCharacteristic productCharacteristic(String productCode, String characteristicType, String characteristicValue) {
    return new ProductCharacteristic(productCode, characteristicType, characteristicValue);
  }

}
