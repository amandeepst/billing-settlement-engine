package com.worldpay.pms.bse.engine.integration;

import static com.worldpay.pms.bse.engine.integration.Db.INPUT_TABLES;
import static com.worldpay.pms.bse.engine.integration.Db.OUTPUT_TABLES;
import static com.worldpay.pms.bse.engine.integration.Db.insertBillCycleSchedule;
import static com.worldpay.pms.bse.engine.integration.Db.insertBillableItem;
import static com.worldpay.pms.bse.engine.integration.Db.insertMiscBillableItemLine;
import static com.worldpay.pms.bse.engine.integration.Db.insertPendingMinimumCharge;
import static com.worldpay.pms.bse.engine.integration.Db.insertPriceCategory;
import static com.worldpay.pms.bse.engine.integration.Db.insertProductCharacteristicView;
import static com.worldpay.pms.bse.engine.integration.Db.insertProductDescription;
import static com.worldpay.pms.bse.engine.integration.Db.insertStdBillableItem;
import static com.worldpay.pms.bse.engine.integration.Db.insertStdBillableItemLine;
import static com.worldpay.pms.bse.engine.integration.Db.insertStdBillableItemSvcQty;
import static com.worldpay.pms.bse.engine.utils.DatabaseCsvUtils.readFromCsvFileAndWriteToExistingTable;
import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.worldpay.pms.bse.engine.utils.WithDatabase;
import com.worldpay.pms.spark.core.PMSException;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import com.worldpay.pms.testing.stereotypes.WithInMemoryDb;
import com.worldpay.pms.testing.stereotypes.WithSparkHeavyUsage;
import com.worldpay.pms.testing.utils.DbUtils;
import java.sql.Date;
import java.util.List;
import org.hamcrest.collection.IsEmptyCollection;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sql2o.Sql2oQuery;
import org.sql2o.data.Row;

@WithInMemoryDb
@WithSparkHeavyUsage
public class BillingEndToEndTest implements WithDatabase {

  private static final String DATE_2021_02_01 = "2021-02-01";
  private static final String DATE_2021_02_17 = "2021-02-17";
  private static final String DATE_2021_02_18 = "2021-02-18";
  private static final String DATE_2021_02_19 = "2021-02-19";
  private static final String DATE_2021_02_28 = "2021-02-28";
  private static final String VALID_FROM_2021_01_01 = "2021-01-01";

  private static final String PROCESSING_GROUP_ALL = "ALL";
  private static final String PROCESSING_GROUP_AUS = "AUSTSYDSTD";
  private static final String BILLING_TYPE_STANDARD_AND_ADHOC = "STANDARD,ADHOC";
  private static final String CHARGE_ACCOUNT_TYPE = "CHRG";
  private static final String FUNDING_ACCOUNT_TYPE = "FUND";

  private static final String WPDY = "WPDY";
  private static final String WPMO = "WPMO";
  private static final String LCP = "PO1100000003";
  private static final String PARTY_ID = "PO4008275082";
  private static final String ACCOUNT_ID = "0300301165";
  private static final String QUERY_CORRECTLY_NEGATED_AMOUNTS = resourceAsString("sql/corrections/get-correctly-negated-amount.sql");
  private static final String QUERY_CORRECTLY_NEGATED_TAX_AMOUNTS = resourceAsString(
      "sql/corrections/get-correctly-negated-tax-amount.sql");
  private static final String QUERY_NOT_NEGATED_TAX_LINES = resourceAsString(
      "sql/corrections/get-not-negated-tax-lines.sql");

  private SqlDb inputDb;
  private JdbcConfiguration inputDbConfig;
  private SqlDb billingDb;
  private SqlDb cisadmDb;

  @Override
  public void bindBillingJdbcConfiguration(JdbcConfiguration conf) {
    billingDb = SqlDb.simple(conf);
    DbUtils.cleanUp(billingDb, OUTPUT_TABLES.toJavaArray(String[]::new));
    DbUtils.cleanUpWithoutMetadata(billingDb, "bill_tax", "bill_tax_detail");
  }

  @Override
  public void bindCisadmJdbcConfiguration(JdbcConfiguration conf) {
    this.cisadmDb = SqlDb.simple(conf);
  }

  @BeforeEach
  void setUp(SqlDb h2Db, JdbcConfiguration h2DbConfig) {
    this.inputDb = h2Db;
    this.inputDbConfig = h2DbConfig;

    this.inputDb.execQuery("mode", "SET MODE Oracle", Sql2oQuery::executeUpdate);
    INPUT_TABLES.forEach(view -> Db.createInputTable(billingDb, inputDb, view));
    DbUtils.cleanUpWithoutMetadata(cisadmDb, "cm_inv_recalc_stg");
  }

  @AfterEach
  void afterEach() {
    DbUtils.cleanUpWithoutMetadata(cisadmDb, "cm_inv_recalc_stg");
  }

  @Test
  void whenNoInputIsPresentThenOutputsWillBeEmpty() {
    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);
    assertOutputCounts(2, 0, 0, 0, 0, 0, 0, 0, 0);
    assertBillPaymentIsPublished(0,0,0);
  }

  @Test
  void whenStandardBillableItemWithDailyBillCycleThenComplete() {
    insertAccount(WPDY, VALID_FROM_2021_01_01, null);
    insertBillCycle(WPDY, DATE_2021_02_17, DATE_2021_02_17);
    insertProductDescription(inputDb, "FPAMC", "Funding - Purchase - Acquired - Mastercard");
    insertStandardBillableItem("6df0e527", DATE_2021_02_17);

    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);

    assertOutputCounts(2, 0, 1, 1, 0, 0, 0, 1, 0);
    assertBillPaymentIsPublished(0,1,1);
  }

  /**
   * Scenario 1
   * GIVEN merchant is set up with minimum charge
   * AND sum of aggregated bill lines is less than configured minimum charge
   * WHEN complete-bill process is executed
   * THEN the delta is created as a minimum charge
   */
  @Test
  void whenMinimumChargeIsSetupAndBillsAreLessThanMinChargeThenDeltaIsCreated() {
    insertAccount(WPDY, null, CHARGE_ACCOUNT_TYPE, ACCOUNT_ID, LCP);
    insertBillCycle(WPMO, DATE_2021_02_01, DATE_2021_02_28);
    insertBillCycle(WPDY, DATE_2021_02_17, DATE_2021_02_17);
    insertBillCycle(WPDY, DATE_2021_02_19, DATE_2021_02_19);
    insertBillCycle(WPDY, DATE_2021_02_28, DATE_2021_02_28);

    insertProductDescription(inputDb, "APUVPMCO", "Acquired - Purchase - MasterCard Corporate (Com)");
    insertProductDescription(inputDb, "MINCHRGP", "Minimum Charge Product");
    insertPriceCategory(inputDb, "PC_MBA", "Percent Charge-Charging Currency-Merchant Proposition Rate");
    insertPriceCategory(inputDb, "TOTAL_BB", "Total charge - billing currency - base FX rate");
    insertProductCharacteristicView(inputDb, "APUVPMCO", "TX_S_KK", "EXEMPT-E");
    insertProductCharacteristicView(inputDb, "MINCHRGP", "TX_S_KK", "EXEMPT-E");
    insertMinimumCharge(inputDb, PARTY_ID, 5000.0, VALID_FROM_2021_01_01, DATE_2021_02_28, LCP);
    insertMinimumCharge(inputDb, PARTY_ID, 6000.0, VALID_FROM_2021_01_01, DATE_2021_02_28, LCP);

    insertStandardBillableItemBillLevelMinCharge("6df0e527", DATE_2021_02_17, ACCOUNT_ID, 1774);
    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);
    assertOutputCounts(2, 0, 1, 1, 0, 0, 1, 1, 1);

    // insert pending min charge in h2 s.t. get-min-charge-as-pending.sql sees the entry inserted in same table in oracle by the runs
    insertPendingMinimumCharge(inputDb, LCP, PARTY_ID, "JPY");

    // this is to check that if no bills are processed in a day, the pending min charges are still carried over
    run(DATE_2021_02_18, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);
    assertOutputCounts(4, 0, 1, 1, 0, 0, 1, 1, 2);

    // this is to check that if we do a run with a diff processing group and we have pending bills with the initial processing group,
    // the pending min charges for the initial processing group are still carried over
    insertStandardBillableItemBillLevelMinCharge("6df0e528", DATE_2021_02_19, ACCOUNT_ID, 1121);
    run(DATE_2021_02_19, PROCESSING_GROUP_AUS, BILLING_TYPE_STANDARD_AND_ADHOC, false);
    assertOutputCounts(6, 1, 1, 2, 0, 0, 1, 1, 3);

    // run again to complete pending daily bill otherwise in the EOM run min charge will be applied to both bills
    run(DATE_2021_02_19, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);
    assertOutputCounts(8, 1, 2, 2, 0, 0, 2, 2, 4);

    insertStandardBillableItemBillLevelMinCharge("6df0e529", DATE_2021_02_28, ACCOUNT_ID, 1000);
    run(DATE_2021_02_28, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);
    assertOutputCounts(10, 1, 3, 3, 0, 0, 4, 4, 4);

    //check the extra bill line has the exact delta amount
    assertMinChargeSumIs(1105.0d);
  }

  /**
   * Scenario 2
   * GIVEN merchant is set up with minimum charge
   * AND sum of aggregated bill lines is more than configured minimum charge
   * WHEN complete-bill process is executed
   * THEN no minimum charge is applied to the merchant
   */
  @Test
  void whenMinimumChargeIsSetupAndBillsAreBiggerThanMinChargeThenNoDeltaIsCreated() {
    insertAccount(WPDY, null, CHARGE_ACCOUNT_TYPE, ACCOUNT_ID, LCP);
    insertBillCycle(WPDY, DATE_2021_02_17, DATE_2021_02_17);
    insertBillCycle(WPMO, DATE_2021_02_17, DATE_2021_02_17);
    insertProductDescription(inputDb, "APUVPMCO", "Acquired - Purchase - MasterCard Corporate (Com)");
    insertProductCharacteristicView(inputDb, "APUVPMCO", "TX_S_KK", "EXEMPT-E");
    insertMinimumCharge(inputDb, PARTY_ID, 10.0, DATE_2021_02_17, DATE_2021_02_17, LCP);
    insertStandardBillableItemBillLevelMinCharge("6df0e527", ACCOUNT_ID, 1774);

    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);

    assertNoMinChargeLineIsCreated();
    assertOutputCounts(2, 0, 1, 1, 0, 0, 1, 1, 0);
  }

  /**
   * Scenario 3
   * GIVEN there are merchants with minimum charge setup
   * AND no bills exists within the minimum charge period
   * WHEN complete-bill process is executed
   * THEN no minimum charge is applied to the merchant
   */
  @Test
  void whenMinimumChargeIsSetupAndNoBillsInTimeframeThenNoDeltaIsCreated() {
    insertAccount(WPDY, null, CHARGE_ACCOUNT_TYPE, ACCOUNT_ID, LCP);
    insertBillCycle(WPDY, DATE_2021_02_17, DATE_2021_02_17);
    insertBillCycle(WPMO, DATE_2021_02_17, DATE_2021_02_17);
    insertProductDescription(inputDb, "APUVPMCO", "Acquired - Purchase - MasterCard Corporate (Com)");
    insertProductCharacteristicView(inputDb, "APUVPMCO", "TX_S_KK", "EXEMPT-E");
    insertMinimumCharge(inputDb, PARTY_ID, 5000.0, "2021-08-25", "2021-08-25", "PO1100000001");
    insertStandardBillableItemBillLevelMinCharge("6df0e527", ACCOUNT_ID, 1774);

    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);

    assertNoMinChargeLineIsCreated();
    assertOutputCounts(2, 0, 1, 1, 0, 0, 1, 1, 0);
  }

  /**
   * Scenario 4
   * GIVEN minimum charge is set up at billing party
   * AND sum of aggregated bill lines for all child parties is less than configured minimum charge
   * WHEN complete-bill process is executed
   * THEN the delta is created as a minimum charge at billing level
   */
  @Test
  void whenMinimumChargePartyLevelAndChildPartiesBillLinesSumLessThanMinChargeThenDeltaIsCreated() {
    insertAccountMinChargeHierarchy("PO4000785312", "8107808305", "PO4000785450", "7971222797");
    insertBillCycle(WPDY, DATE_2021_02_17, DATE_2021_02_17);
    insertBillCycle(WPMO, DATE_2021_02_17, DATE_2021_02_17);
    insertProductDescription(inputDb, "APUVPMCO", "Acquired - Purchase - MasterCard Corporate (Com)");
    insertProductDescription(inputDb, "MINCHRGP", "Minimum Charge Product");
    insertPriceCategory(inputDb, "PC_MBA", "Percent Charge-Charging Currency-Merchant Proposition Rate");
    insertPriceCategory(inputDb, "TOTAL_BB", "Total charge - billing currency - base FX rate");
    insertProductCharacteristicView(inputDb, "APUVPMCO", "TX_S_UK", "EXEMPT-E");
    insertProductCharacteristicView(inputDb, "MINCHRGP", "TX_S_UK", "EXEMPT-E");
    insertMinimumCharge(inputDb, "PO4000785312", 5000.0, DATE_2021_02_17, DATE_2021_02_17, "PO1100000001");
    insertStandardBillableItemChildLevelMinCharge("6df0e527X", "7971222797", 111);
    insertStandardBillableItemChildLevelMinCharge("6df0e527Y", "7971222797", 333);

    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);

    //check the extra bill line has the exact delta amount
    assertMinChargeSumIs(4556.0d);
    assertOutputCounts(2, 0, 1, 2, 0, 0, 2, 2, 0);
  }

  @Test
  void whenMinimumChargePartyLevelAndChildPartiesBillLinesSumNegativeValueThenDeltaIsCreated() {
    insertAccountMinChargeHierarchy("PO4000785312", "8107808305", "PO4000785450", "7971222797");
    insertBillCycle(WPDY, DATE_2021_02_17, DATE_2021_02_17);
    insertBillCycle(WPMO, DATE_2021_02_17, DATE_2021_02_17);
    insertProductDescription(inputDb, "APUVPMCO", "Acquired - Purchase - MasterCard Corporate (Com)");
    insertProductDescription(inputDb, "MINCHRGP", "Minimum Charge Product");
    insertPriceCategory(inputDb, "PC_MBA", "Percent Charge-Charging Currency-Merchant Proposition Rate");
    insertPriceCategory(inputDb, "TOTAL_BB", "Total charge - billing currency - base FX rate");
    insertProductCharacteristicView(inputDb, "APUVPMCO", "TX_S_UK", "EXEMPT-E");
    insertProductCharacteristicView(inputDb, "MINCHRGP", "TX_S_UK", "EXEMPT-E");
    insertMinimumCharge(inputDb, "PO4000785312", 5000.0, DATE_2021_02_17, DATE_2021_02_17, "PO1100000001");
    insertStandardBillableItemChildLevelMinCharge("6df0e527X", "7971222797", 111);
    insertStandardBillableItemChildLevelMinCharge("6df0e527Y", "7971222797", -333);

    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);

    assertMinChargeSumIs(5000.0d);
    assertOutputCounts(2, 0, 1, 2, 0, 0, 2, 2, 0);
  }

  /**
   * Scenario 5
   * GIVEN minimum charge is set up at billing party
   * AND sum of aggregated bill lines for all child parties is more than configured minimum charge
   * WHEN complete-bill process is executed
   * THEN no minimum charge is applied to the merchant
   */
  @Test
  void whenMinimumChargePartyLevelAndChildPartiesBillLinesSumMoreThanMinChargeThenNoMinChargeIsApplied() {
    insertAccountMinChargeHierarchy("PO4000785312", "8107808305", "PO4000785450", "7971222797");
    insertBillCycle(WPDY, DATE_2021_02_17, DATE_2021_02_17);
    insertBillCycle(WPMO, DATE_2021_02_17, DATE_2021_02_17);
    insertProductDescription(inputDb, "APUVPMCO", "Acquired - Purchase - MasterCard Corporate (Com)");
    insertProductDescription(inputDb, "MINCHRGP", "Minimum Charge Product");
    insertPriceCategory(inputDb, "PC_MBA", "Percent Charge-Charging Currency-Merchant Proposition Rate");
    insertPriceCategory(inputDb, "TOTAL_BB", "Total charge - billing currency - base FX rate");
    insertProductCharacteristicView(inputDb, "APUVPMCO", "TX_S_UK", "EXEMPT-E");
    insertProductCharacteristicView(inputDb, "MINCHRGP", "TX_S_UK", "EXEMPT-E");
    insertMinimumCharge(inputDb, "PO4000785312", 30.0, DATE_2021_02_17, DATE_2021_02_17, "PO1100000001");
    insertStandardBillableItemChildLevelMinCharge("6df0e527X", "7971222797", 1774);

    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);

    assertNoMinChargeLineIsCreated();
    assertOutputCounts(2, 0, 1, 1, 0, 0, 1, 1, 0);
  }

  /**
   * Scenario 6
   * GIVEN minimum charge is set up at billing party
   * AND no bills exists within the minimum charge period
   * WHEN complete-bill process is executed
   * THEN minimum charge is applied to the merchant and a new bill is created
   */
  @Test
  void whenMinimumChargePartyLevelAndNoBillsWithinPeriodThenMinChargeIsApplied() {
    insertAccount(WPDY, null, CHARGE_ACCOUNT_TYPE, ACCOUNT_ID, LCP);
    insertBillCycle(WPDY, DATE_2021_02_17, DATE_2021_02_17);
    insertBillCycle(WPMO, DATE_2021_02_17, DATE_2021_02_17);
    insertMinimumCharge(inputDb, PARTY_ID, 3000.0, DATE_2021_02_17, DATE_2021_02_17, LCP);
    insertProductCharacteristicView(inputDb, "MINCHRGP", "TX_S_KK", "EXEMPT-E");
    insertProductDescription(inputDb, "MINCHRGP", "Minimum Charge Product");

    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);
    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);

    assertMinChargeSumIs(3000d);
    assertOutputCounts(4, 0, 1, 0, 0, 0, 2, 1, 0);
  }

  /**
   * Scenario 7
   * GIVEN minimum charge is set up at child parties
   * AND sum of aggregated bill lines for child party is less than configured minimum charge
   * WHEN complete-bill process is executed
   * THEN the delta is created as a minimum charge at child level
   */
  @Test
  void whenMinChargeChildLevelAndBillLinesLessThanMinChargeThenDeltaIsAdded() {
/*      -- Parent Party Id:       PO4000785312      -- Child Party Id:        PO4000785450
        -- Parent Account Id:     8107802301        -- Child Account Id:      7971222362
        -- Parent Sub Account Id: 8107808305        -- Child Sub Account Id:  7971222797        */
    insertAccountMinChargeHierarchy("PO4000785312", "8107808305", "PO4000785450", "7971222797");
    insertBillCycle(WPDY, DATE_2021_02_17, DATE_2021_02_17);
    insertBillCycle(WPMO, DATE_2021_02_17, DATE_2021_02_17);
    insertProductDescription(inputDb, "APUVPMCO", "Acquired - Purchase - MasterCard Corporate (Com)");
    insertProductDescription(inputDb, "MINCHRGP", "Minimum Charge Product");
    insertPriceCategory(inputDb, "PC_MBA", "Percent Charge-Charging Currency-Merchant Proposition Rate");
    insertPriceCategory(inputDb, "TOTAL_BB", "Total charge - billing currency - base FX rate");
    insertProductCharacteristicView(inputDb, "APUVPMCO", "TX_S_UK", "EXEMPT-E");
    insertProductCharacteristicView(inputDb, "MINCHRGP", "TX_S_UK", "EXEMPT-E");
    insertMinimumCharge(inputDb, "PO4000785450", 5000.0, DATE_2021_02_17, DATE_2021_02_17, "PO1100000001");
    insertStandardBillableItemChildLevelMinCharge("6df0e527", "8107808305", 1774);
    insertStandardBillableItemChildLevelMinCharge("6df0e527X", "7971222797", 444);

    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);

    //check the extra bill line has the exact delta amount
    assertMinChargeSumIs(4556.0d);
    assertOutputCounts(2, 0, 1, 2, 0, 0, 2, 3, 0);
  }

  /**
   * Scenario 8
   * GIVEN minimum charge is set up at child parties
   * AND sum of aggregated bill lines for all child party is more than configured minimum charge
   * WHEN complete-bill process is executed
   * THEN no minimum charge is applied at child level
   */
  @Test
  void whenMinChargeChildLevelAndBillLinesMoreThanMinChargeThenNoDeltaIsCreated() {
    insertAccountMinChargeHierarchy("PO4000785312", "8107808305", "PO4000785450", "7971222797");
    insertBillCycle(WPDY, DATE_2021_02_17, DATE_2021_02_17);
    insertBillCycle(WPMO, DATE_2021_02_17, DATE_2021_02_17);
    insertProductDescription(inputDb, "APUVPMCO", "Acquired - Purchase - MasterCard Corporate (Com)");
    insertProductDescription(inputDb, "MINCHRGP", "Minimum Charge Product");
    insertPriceCategory(inputDb, "PC_MBA", "Percent Charge-Charging Currency-Merchant Proposition Rate");
    insertPriceCategory(inputDb, "TOTAL_BB", "Total charge - billing currency - base FX rate");
    insertProductCharacteristicView(inputDb, "APUVPMCO", "TX_S_UK", "EXEMPT-E");
    insertProductCharacteristicView(inputDb, "MINCHRGP", "TX_S_UK", "EXEMPT-E");
    insertMinimumCharge(inputDb, "PO4000785450", 300.0, DATE_2021_02_17, DATE_2021_02_17, "PO1100000001");
    insertStandardBillableItemChildLevelMinCharge("6df0e527", "8107808305", 1774);
    insertStandardBillableItemChildLevelMinCharge("6df0e527X", "7971222797", 444);

    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);
    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);

    assertNoMinChargeLineIsCreated();
    assertOutputCounts(4, 0, 1, 2, 0, 0, 1, 2, 0);
  }

  /**
   * Scenario 9
   * GIVEN minimum charge is set up at child parties
   * AND no bills exists within the minimum charge period
   * WHEN complete-bill process is executed
   * THEN minimum charge is applied at child level
   */
  @Test
  void whenMinChargeChildLevelAndNoBillAtChildLevelThenDeltaIsCreated() {
    insertAccountMinChargeHierarchy("PO4000785312", "8107808305", "PO4000785450", "7971222797");
    insertBillCycle(WPDY, DATE_2021_02_17, DATE_2021_02_17);
    insertBillCycle(WPMO, DATE_2021_02_17, DATE_2021_02_17);
    insertProductDescription(inputDb, "APUVPMCO", "Acquired - Purchase - MasterCard Corporate (Com)");
    insertProductDescription(inputDb, "MINCHRGP", "Minimum Charge Product");
    insertPriceCategory(inputDb, "PC_MBA", "Percent Charge-Charging Currency-Merchant Proposition Rate");
    insertPriceCategory(inputDb, "TOTAL_BB", "Total charge - billing currency - base FX rate");
    insertProductCharacteristicView(inputDb, "APUVPMCO", "TX_S_UK", "EXEMPT-E");
    insertProductCharacteristicView(inputDb, "MINCHRGP", "TX_S_UK", "EXEMPT-E");
    insertMinimumCharge(inputDb, "PO4000785450", 5000.0, DATE_2021_02_17, DATE_2021_02_17, "PO1100000001");
    insertStandardBillableItemChildLevelMinCharge("6df0e527", "8107808305", 1774);

    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);

    assertMinChargeSumIs(5000d);
    assertOutputCounts(2, 0, 1, 1, 0, 0, 2, 2, 0);
  }

  @Test
  void whenMinChargeChildLevelAndOnlyMiscBillSeparateBillIsCreated() {
    insertAccountMinChargeHierarchy("PO4000785312", "8107808305", "PO4000785450", "7971222797");
    insertBillCycle(WPDY, DATE_2021_02_17, DATE_2021_02_17);
    insertBillCycle(WPMO, DATE_2021_02_17, DATE_2021_02_17);
    insertProductDescription(inputDb, "MIGCHRG", "Migrated Charge");
    insertProductDescription(inputDb, "MINCHRGP", "Minimum Charge Product");
    insertProductCharacteristicView(inputDb, "MIGCHRG", "TX_S_UK", "EXEMPT-E");
    insertProductCharacteristicView(inputDb, "MINCHRGP", "TX_S_UK", "EXEMPT-E");
    insertMinimumCharge(inputDb, "PO4000785450", 5000.0, DATE_2021_02_17, DATE_2021_02_17, "PO1100000001");

    insertMiscBillableItem("U0drXCouR5y-", "8107808305", "2021-02-17", "Y");

    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);

    assertMinChargeSumIs(5000d);
    assertOutputCounts(2, 0, 2, 1, 0, 0, 4, 2, 0);
  }

  @Test
  void whenMiscBillableItemWithAdhocFlagTrueThenComplete() {
    insertAccount(WPMO, VALID_FROM_2021_01_01, null);
    insertBillCycle(WPMO, DATE_2021_02_01, DATE_2021_02_28);
    insertProductDescription(inputDb, "MIGCHRG", "Migrated Charge");

    insertMiscBillableItem("U0drXCouR5y-", "2021-02-17", "Y");

    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);

    assertOutputCounts(2, 0, 1, 1, 0, 0, 1, 1, 0);
    assertBillPaymentIsPublished(1,1,1);
  }

  /*
   * 12./13.
   * Given a transaction with a monthly bill cycle where the Account validity date <= bill cycle end date and Accrued_dt = Account validity date,
   * When billing runs for a date prior to the last day of the bill cycle,
   * Then the transaction should be added to the pending bill while bill completion will take place only when billing runs for the last day of the bill cycle.
   *
   * 14.
   * Given a transaction where the Accrued_dt equals the Account valid to date,
   * When billing runs for the last day of the month,
   * Then a complete bill should be generated.
   *
   * 15.
   * Given a transaction with Account Override N and Accrued_DT > Account valid to date and a monthly bill cycle,
   * When billing runs,
   * Then this transaction should follow the error process
   */
  @Test
  void accountValidityScenarios() {
    insertAccount(WPMO, VALID_FROM_2021_01_01, DATE_2021_02_17);
    insertBillCycle(WPMO, DATE_2021_02_01, DATE_2021_02_28);
    insertProductDescription(inputDb, "FPAMC", "Funding - Purchase - Acquired - Mastercard");

    // tx accrued date = acct validity end => pending bill
    insertStandardBillableItem("6df0e527", DATE_2021_02_17);

    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);
    assertOutputCounts(2, 1, 0, 1, 0, 0, 0, 0, 0);

    // tx accrued date > acct validity end => tx error
    insertStandardBillableItem("6df0e528", DATE_2021_02_18);

    run(DATE_2021_02_18, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);
    assertOutputCounts(4, 2, 0, 1, 1, 0, 0, 0, 0);

    // logical date = bill cycle end => complete pending bill even if account is not valid anymore
    run(DATE_2021_02_28, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);
    assertOutputCounts(6, 2, 1, 1, 1, 0, 0, 1, 0);
    assertBillPaymentIsPublished(0,1,1);
  }

  /**
   * 21. Given a transaction with Date Override Y, the bill cycle is monthly and Accrued_dt equals the run date, When billing runs for a
   * date before the last day of the month, Then the bill should not be generated
   * <p>
   * 23. Given a transaction with Date Override Y, the bill cycle is monthly and Accrued_dt a month after the run date, When billing runs
   * for a date before the last day of the month, Then the cycle start and end date should be determined based on the Accrued_dt and the
   * bill should not be generated
   * <p>
   * 24. Given a transaction with Date Override Y, the bill cycle is monthly and Accrued_dt is one month earlier than the run date, When
   * billing runs for a date before the last day of the month, Then the cycle start date and end date should be determined based on the job
   * parameter and the bill should not be generated
   */
  @Test
  void accruedDateOverrideScenarios() {
    insertAccount(WPMO, VALID_FROM_2021_01_01, null);
    insertBillCycle(WPMO, VALID_FROM_2021_01_01, "2021-01-31");
    insertBillCycle(WPMO, DATE_2021_02_01, DATE_2021_02_28);
    insertBillCycle(WPMO, "2021-03-01", "2021-03-31");
    insertProductDescription(inputDb, "FPAMC", "Funding - Purchase - Acquired - Mastercard");

    insertStandardBillableItem("6df0e527", "2021-01-30");
    insertStandardBillableItem("6df0e528", "2021-02-17");
    insertStandardBillableItem("6df0e529", "2021-03-02");

    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, true);
    assertOutputCounts(2, 2, 0, 3, 0, 0,0, 0, 0);
    assertBillPaymentIsPublished(0,0,0);
    assertThat(getPendingBill("6df0e527").getString("end_dt"), is("2021-02-28 00:00:00.0"));
    assertThat(getPendingBill("6df0e528").getString("end_dt"), is("2021-02-28 00:00:00.0"));
    assertThat(getPendingBill("6df0e529").getString("end_dt"), is("2021-03-31 00:00:00.0"));
  }


  /**
   * Scenario 1 cancel
   * GIVEN a cancel bill record is added
   * WHEN complete-bill process is executed
   * THEN a new cancel is created with the reverse values of the canceled one
   */
  @Test
  void whenCancelBillRecordExistsThanFullCancelBillCreated() {
    //insert cancel records in cm_inv_recalc_stg
    readFromCsvFilesAndWriteToExistingTables("input/corrections_scen1/");

    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);

    assertOutputCounts(2, 0, 8, 0, 0, 0, 5, 35, 0);
    assertAllRequiredBillsAreNegated();
    assertAllRequiredAmountsAreNegated();
    assertNegatedAmounts();
    assertAllRequiredTaxLinesAreNegated();
    assertNegatedTaxAmounts();
  }

  /**
   * Scenario 2 Missing Bill
   * GIVEN a cancel bill record is added
   * AND There is no bill with the same ID
   * WHEN complete-bill process is executed
   * THEN an error is thrown
   */
  @Test
  void whenCancelBillRecordExistsAndNoBillWithThatIdThanErrorIsThrown() {
    //insert cancel bill record in cm_inv_recalc_stg but no bill for it
    readFromCsvFileAndWriteToExistingTable(cisadmDb, "input/corrections_scen2/cm_inv_recalc_stg.csv", "cm_inv_recalc_stg");

    PMSException pmsException = assertThrows(PMSException.class,
        () -> run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false),
        "The app should have thrown an exception for exceeding the threshold");

    Assert.assertEquals(
        "Wrong message thrown by the app for exceeding the threshold",
        "Driver interrupted, error 'bills' exceed percent threshold of '25.0%', with '1' errors from '1' total",
        pmsException.getMessage()
    );
    assertOutputCounts(2, 0, 0, 0, 0, 1, 0, 0, 0);
  }

  /**
   * Scenario 3 cancel
   * GIVEN a cancel bill record is added for an invalid bill
   * WHEN complete-bill process is executed
   * THEN an error is shown
   */
  @Test
  void whenCancelBillRecordForIncompleteBillThanErrorIsCreated() {
    //insert cancel records in cm_inv_recalc_stg
    readFromCsvFilesAndWriteToExistingTables("input/corrections_scen3/");

    run(DATE_2021_02_17, PROCESSING_GROUP_ALL, BILLING_TYPE_STANDARD_AND_ADHOC, false);

    assertOutputCounts(2, 0, 16, 0, 0, 1, 3, 31, 0);
  }



  private void readFromCsvFilesAndWriteToExistingTables(String inputDataSetRootDir) {
    readFromCsvFileAndWriteToExistingTable(billingDb, inputDataSetRootDir + "bill.csv", "bill");
    readFromCsvFileAndWriteToExistingTable(billingDb, inputDataSetRootDir + "bill_line.csv", "bill_line");
    readFromCsvFileAndWriteToExistingTable(billingDb, inputDataSetRootDir + "bill_line_calc.csv", "bill_line_calc");
    readFromCsvFileAndWriteToExistingTable(billingDb, inputDataSetRootDir + "bill_line_svc_qty.csv", "bill_line_svc_qty");
    readFromCsvFileAndWriteToExistingTable(cisadmDb, inputDataSetRootDir + "cm_inv_recalc_stg.csv", "cm_inv_recalc_stg");
    readFromCsvFileAndWriteToExistingTable(billingDb, inputDataSetRootDir + "bill_tax.csv", "bill_tax");
    readFromCsvFileAndWriteToExistingTable(billingDb, inputDataSetRootDir + "bill_tax_detail.csv", "bill_tax_detail");
  }

  private void assertNegatedAmounts() {
    List<Integer> correctlyNegatedAmounts = getCorrectlyNegatedAmounts(billingDb);
    assertThat(correctlyNegatedAmounts.stream().allMatch(correct -> correct == 1), is(true));
  }

  private void assertNegatedTaxAmounts() {
    List<Integer> correctlyNegatedAmounts = getCorrectlyNegatedTaxAmounts(billingDb);
    assertThat(correctlyNegatedAmounts.stream().allMatch(correct -> correct == 1), is(true));
  }

  private void assertAllRequiredBillsAreNegated() {
    List<String> notNegatedBills = getNotCanceledBIllIds(billingDb);
    assertThat("All these bills should have been negated! "+ notNegatedBills, notNegatedBills, IsEmptyCollection.empty());
  }
  private void assertAllRequiredAmountsAreNegated() {
    List<String> notNegatedBills = getNotNegatedAmounts(billingDb);
    assertThat("All these amounts should have been negated! " + notNegatedBills, notNegatedBills, IsEmptyCollection.empty());
  }

  private void assertAllRequiredTaxLinesAreNegated() {
    List<String> notNegatedBills = getNotNegatedTaxLines(billingDb);
    assertThat("All these tax lines should have been negated! " + notNegatedBills, notNegatedBills, IsEmptyCollection.empty());
  }

  public List<Integer> getCorrectlyNegatedAmounts(SqlDb db) {
    return db.execQuery("correctly-negated-amounts", QUERY_CORRECTLY_NEGATED_AMOUNTS,
        (query) -> query.executeAndFetch(Integer.class));
  }
  public List<Integer> getCorrectlyNegatedTaxAmounts(SqlDb db) {
    return db.execQuery("correctly-negated-tax-amounts", QUERY_CORRECTLY_NEGATED_TAX_AMOUNTS,
        (query) -> query.executeAndFetch(Integer.class));
  }
  public List<String> getNotCanceledBIllIds(SqlDb db) {
    return db.execQuery("not-canceled-bill-ids",
        "select bill_id from cm_inv_recalc_stg " +
            "where cm_inv_recalc_stg.bill_id not in (select prev_bill_id from bill)",
        (query) -> query.executeAndFetch(String.class));
  }

  public List<String> getNotNegatedAmounts(SqlDb db) {
    return db.execQuery("not-negated-amounts",
        "select bill_id from cm_inv_recalc_stg " +
            "where cm_inv_recalc_stg.bill_id not in (select prev_bill_id from bill)",
        (query) -> query.executeAndFetch(String.class));
  }

  public List<String> getNotNegatedTaxLines(SqlDb db) {
    return db.execQuery("not-negated-amounts", QUERY_NOT_NEGATED_TAX_LINES,
        (query) -> query.executeAndFetch(String.class));
  }

  private List<Long> getPartitionsFromTable(SqlDb db, String table) {
    return db.execQuery("get partition column from " + table, "SELECT partition FROM " + table,
        (query) -> query.executeAndFetch(Long.class));
  }

  private void insertAccount(String billCycleId, String validFrom, String validTo) {
    Db.insertAccountDataNoHier(inputDb, "0300301165", FUNDING_ACCOUNT_TYPE, "0300586258", FUNDING_ACCOUNT_TYPE, "PO4008275082", "00018",
        "JPN", "JPY", "PO1300000002", null, billCycleId, "ACTIVE", validFrom, validTo);
  }

  private void insertAccount(String billCycleId, String validTo, String accountType, String subAccountId, String legalCounterparty) {
    Db.insertAccountDataNoHier(inputDb, subAccountId, accountType, "0300586258", accountType, "PO4008275082",
        legalCounterparty, "JPN", "JPY", "PO1300000002", null, billCycleId, "ACTIVE", VALID_FROM_2021_01_01, validTo);
  }

  private void insertAccountMinChargeHierarchy(String parentPartyId, String parentSubAccountId, String childPartyId, String childSubAccountId) {
/*      -- Parent Party Id:       PO4000785312      -- Child Party Id:        PO4000785450
        -- Parent Account Id:     8107802301        -- Child Account Id:      7971222362
        -- Parent Sub Account Id: 8107808305        -- Child Sub Account Id:  7971222797        */
    Db.insertAccountDataWithHier(inputDb, parentSubAccountId, CHARGE_ACCOUNT_TYPE, "8107802301", CHARGE_ACCOUNT_TYPE, parentPartyId,
        childSubAccountId, CHARGE_ACCOUNT_TYPE,"7971222362", CHARGE_ACCOUNT_TYPE, childPartyId,
        "PO1100000001","GBR", "JPY", "PO1300000002", null, BillingEndToEndTest.WPDY, "ACTIVE", VALID_FROM_2021_01_01, null);
  }

  private void insertBillCycle(String billCycleCode, String windowsStart, String windowEnd) {
    insertBillCycleSchedule(inputDb, billCycleCode, windowsStart, windowEnd);
  }

  private void insertStandardBillableItemChildLevelMinCharge(String billableItemId, String subAccountId, int preciseChargeAmount) {
    insertStandardBillableItem(billableItemId, DATE_2021_02_17, subAccountId, "PO1100000001", "JPY",
        preciseChargeAmount, "ACQUIRED", "APUVPMCO", "FND_AMTM");
  }

  private void insertStandardBillableItemBillLevelMinCharge(String billableItemId, String accruedDate, String subAccountId,
      int preciseChargeAmount) {
    insertStandardBillableItem(billableItemId, accruedDate, subAccountId, "PO1100000001", "JPY",
        preciseChargeAmount, "ACQUIRED", "APUVPMCO", "FND_AMTM");
  }

  private void insertStandardBillableItemBillLevelMinCharge(String billableItemId, String subAccountId, int preciseChargeAmount) {
    insertStandardBillableItemBillLevelMinCharge(billableItemId, DATE_2021_02_17, subAccountId, preciseChargeAmount);
  }

  private void insertStandardBillableItem(String billableItemId, String accruedDate) {
    insertStandardBillableItem(billableItemId, accruedDate, ACCOUNT_ID, LCP, "JPY", 1774, "ACQUIRED", "FPAMC", "FND_AMTM");
  }

  private void insertStandardBillableItem(String billableItemId, String accruedDate, String subAccountId, String legalCounterparty,
      String currency, int preciseChargeAmount, String productClass, String childProduct, String characteristicValue) {
    insertBillableItem(
        () -> insertStdBillableItem(inputDb, billableItemId, subAccountId, legalCounterparty, accruedDate, "FNDAQRD",
            "9211159046", null, null, currency, currency, currency, "JPY", currency,
            "2101260300586258", productClass, childProduct, 868952478),
        () -> insertStdBillableItemLine(inputDb, billableItemId, "SETT-CTL  ", preciseChargeAmount, characteristicValue,
            "MSC_PI", 1),
        () -> insertStdBillableItemSvcQty(inputDb, billableItemId, "F_B_MFX", "FUNDCQPR", 0.0391588),
        () -> insertStdBillableItemSvcQty(inputDb, billableItemId, "P_B_EFX", "FUNDCQPR", 1),
        () -> insertStdBillableItemSvcQty(inputDb, billableItemId, "AP_B_EFX", "FUNDCQPR", 1),
        () -> insertStdBillableItemSvcQty(inputDb, billableItemId, "TXN_AMT", "FUNDCQPR", preciseChargeAmount),
        () -> insertStdBillableItemSvcQty(inputDb, billableItemId, "TXN_VOL", "FUNDCQPR", 2)
    );
  }

  private void insertMiscBillableItem(String billableItemId, String accruedDate, String adhocBillFlag){
    insertMiscBillableItem(billableItemId, ACCOUNT_ID, accruedDate, adhocBillFlag);
  }

  private void insertMiscBillableItem(String billableItemId, String subAccountId, String accruedDate, String adhocBillFlag) {
    insertBillableItem(
        () -> Db.insertMiscBillableItem(inputDb, billableItemId, subAccountId, LCP, accruedDate, adhocBillFlag,
            "JPY", "PROCESSED", "MIGCHRG", 1, "N", "N", "N", "N", "N",
            null, null, "REC-CHG", "1059759", "ACTIVE"),
        () -> insertMiscBillableItemLine(inputDb, billableItemId, "PI_MBA", 125, 125)
    );
  }

  private void insertMinimumCharge(SqlDb inputDb, String partyId, double rate, String start_dt, String end_dt, String lcp) {
    Db.insertMinimumCharge(inputDb, "7970974678", lcp,
        partyId, WPMO, "MIN_P_CHRG",
        rate, start_dt, end_dt, "ACTV", "JPY");
  }

  private Row getBillLineDetail(String billableItemId) {
    return Db.getBillLineDetail(billingDb, billableItemId);
  }

  private Row getPendingBill(String billableItemId) {
    return Db.getPendingBill(billingDb, billableItemId);
  }

  private void assertOutputCounts(long batchHistoryCount, long pendingBillCount, long completeBillCount, long billLineDetailCount,
      long billableItemErrorCount, long billErrorCount, long billPriceCount, long billAccountingCount, long pendingMinChargeCount) {
    assertEquals(batchHistoryCount, DbUtils.getCount(billingDb, "batch_history"),
        "There should be " + batchHistoryCount + " lines in batch_history table");
    assertEquals(billableItemErrorCount, DbUtils.getCount(billingDb, "bill_item_error"),
        "There should be " + billableItemErrorCount + " lines in bill_item_error table");
    assertEquals(billErrorCount, DbUtils.getCount(billingDb, "bill_error"),
        "There should be " + billErrorCount + " lines in bill_error table");
    assertEquals(pendingBillCount, DbUtils.getCount(billingDb, "pending_bill"),
        "There should be " + pendingBillCount + " lines in pending_bill table");
    assertEquals(completeBillCount, DbUtils.getCount(billingDb, "bill"),
        "There should be " + completeBillCount + " lines in bill table");
    assertEquals(billLineDetailCount, DbUtils.getCount(billingDb, "bill_line_detail"),
        "There should be " + billLineDetailCount + " lines in bill_line_detail table");
    assertEquals(billPriceCount, DbUtils.getCount(billingDb, "bill_price"),
        "There should be " + billPriceCount + " lines in bill_price table");
    assertEquals(billAccountingCount, DbUtils.getCount(billingDb, "bill_accounting"),
        "There should be " + billAccountingCount + " lines in bill_accounting table");
    assertEquals(pendingMinChargeCount, DbUtils.getCount(billingDb, "pending_min_charge"),
        "There should be " + pendingMinChargeCount + " lines in pending_min_charge table");
  }

  private void assertMinChargeSumIs(double sum) {
    assertThat("There should be at least 1 bill line", DbUtils.getCount(billingDb, "bill_line"), greaterThanOrEqualTo(1L));
    assertThat("There should be at least 1 bill line for minimum charge", getCountMinChargeBillLines(billingDb), greaterThanOrEqualTo(1L));
    assertThat("Min Charge sum should be " + sum, getMinChargeAmount(billingDb), closeTo(sum, 0.001d));
  }

  private void assertNoMinChargeLineIsCreated() {
    assertThat("There should be NO bill line for minimum charge", getCountMinChargeBillLines(billingDb), is(0L));
  }

  public static long getCountMinChargeBillLines(SqlDb db) {
    return db.execQuery("count-min-charge-bill-lines", "SELECT COUNT(*) FROM bill_line where product_id = 'MINCHRGP'",
        (query) -> query.executeScalar(Long.TYPE));
  }

  public static double getMinChargeAmount(SqlDb db) {
    return db.execQuery("min-charge-amount", "select sum(total_amount) from bill_line where product_id = 'MINCHRGP'",
        (query) -> query.executeScalar(Double.TYPE));
  }

  private void assertBillPaymentIsPublished(long billDueDateCount, long billPaymentCount, long billPaymentSnapshotCount){
    assertEquals(billDueDateCount, DbUtils.getCount(billingDb, "cm_bill_due_dt"));
    assertEquals(billPaymentCount, DbUtils.getCount(billingDb, "cm_bill_payment_dtl"));
    assertEquals(billPaymentSnapshotCount, DbUtils.getCount(billingDb, "cm_bill_payment_dtl_snapshot"));
  }

  private void run(String logicalDate, String processingGroup, String billingType, boolean overrideEventDate) {
    TestBillingEngine.submit(inputDbConfig, Date.valueOf(logicalDate), processingGroup, billingType, overrideEventDate);
  }
}
