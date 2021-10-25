package com.worldpay.pms.bse.engine.data;

import static com.worldpay.pms.bse.engine.integration.Db.deleteProductCharacteristic;
import static com.worldpay.pms.bse.engine.integration.Db.insertProductCharacteristic;
import static com.worldpay.pms.testing.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.iterableWithSize;

import com.worldpay.pms.bse.domain.account.BillingCycle;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumCharge;
import com.worldpay.pms.bse.domain.model.tax.ProductCharacteristic;
import com.worldpay.pms.bse.domain.staticdata.Currency;
import com.worldpay.pms.bse.engine.utils.WithDatabase;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.utils.DbUtils;
import java.math.BigDecimal;
import java.sql.Date;
import org.junit.jupiter.api.Test;

class JdbcStaticDataRepositoryTest implements WithDatabase {

  private static final Date LOGICAL_DATE = Date.valueOf("2021-03-25");

  private SqlDb inputDb;
  private SqlDb refDb;

  private JdbcStaticDataRepository staticDataRepository;

  @Override
  public void bindCisadmJdbcConfiguration(JdbcConfiguration conf) {
    this.inputDb = SqlDb.simple(conf);
  }

  @Override
  public void bindRefDataJdbcConfiguration(JdbcConfiguration conf) {
    this.refDb = SqlDb.simple(conf);
  }

  public void bindBillingJdbcConfiguration(JdbcConfiguration conf) {
    staticDataRepository = new JdbcStaticDataRepository(conf, LOGICAL_DATE);
  }

  @Test
  void whenGetProductCharacteristicsThenReturnListOfProductCharacteristics() {
    deleteProductCharacteristic(refDb, "TSTCCC06", "TX_S_ROI", "EXEMPT-E");
    insertProductCharacteristic(refDb, "TSTCCC06", "TX_S_ROI", "EXEMPT-E");

    Iterable<ProductCharacteristic> productCharacteristics = staticDataRepository.getProductCharacteristics();

    deleteProductCharacteristic(refDb, "TSTCCC06", "TX_S_ROI", "EXEMPT-E");
    assertThat(productCharacteristics, hasItem(productCharacteristic("TSTCCC06", "TX_S_ROI", "EXEMPT-E")));
  }

  @Test
  void testCanFetchCurrencyAndCalcTypeDisplay() {
    DbUtils.cleanUpWithoutMetadata(inputDb, "ci_currency_cd");
    readFromCsvFileAndWriteToExistingTable(inputDb, "input/static-data/ci_currency_cd.csv", "ci_currency_cd");
    Iterable<Currency> currencies = staticDataRepository.getDatabaseCurrencies();

    assertThat(currencies, iterableWithSize(3));
    assertThat(currencies, hasItem(new Currency("GBP", (short) 2)));
    assertThat(currencies, hasItem(new Currency("JPY", (short) 3)));
    assertThat(currencies, hasItem(new Currency("MYR", (short) 0)));
  }

  @Test
  void whenGetMinimumChargeThenReturnCorrectList() {
    DbUtils.cleanUpWithoutMetadata(inputDb, "ci_per");
    DbUtils.cleanUpWithoutMetadata(inputDb, "ci_per_char");
    DbUtils.cleanUpWithoutMetadata(inputDb, "ci_per_id");
    DbUtils.cleanUpWithoutMetadata(inputDb, "ci_priceasgn");
    DbUtils.cleanUpWithoutMetadata(inputDb, "ci_pricecomp");
    DbUtils.cleanUpWithoutMetadata(inputDb, "ci_party");

    readFromCsvFileAndWriteToExistingTable(inputDb, "input/static-data/ci_per.csv", "ci_per");
    readFromCsvFileAndWriteToExistingTable(inputDb, "input/static-data/ci_per_char.csv", "ci_per_char");
    readFromCsvFileAndWriteToExistingTable(inputDb, "input/static-data/ci_per_id.csv", "ci_per_id");
    readFromCsvFileAndWriteToExistingTable(inputDb, "input/static-data/ci_priceasgn.csv", "ci_priceasgn");
    readFromCsvFileAndWriteToExistingTable(inputDb, "input/static-data/ci_pricecomp.csv", "ci_pricecomp");
    readFromCsvFileAndWriteToExistingTable(inputDb, "input/static-data/ci_party.csv", "ci_party");

    Iterable<MinimumCharge> minimumCharges = staticDataRepository.getMinimumCharge();
    assertThat(minimumCharges, iterableWithSize(2));
    assertThat(minimumCharges, hasItem(
        new MinimumCharge("1595655016", "PO1100000001", "PO4000018151", "MIN_P_CHRG", BigDecimal.valueOf(15L), "GBP",
            new BillingCycle("WPMO", null, null))));
    assertThat(minimumCharges, hasItem(
        new MinimumCharge("1595655018", "PO1100000001", "PO4000018151", "MIN_P_CHRG", BigDecimal.valueOf(15L), "GBP",
            new BillingCycle("WPMO", null, null))));
  }

  private static ProductCharacteristic productCharacteristic(String productCode, String characteristicType, String characteristicValue) {
    return new ProductCharacteristic(productCode, characteristicType, characteristicValue);
  }
}