package com.worldpay.pms.bse.engine.integration;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.fail;

import com.worldpay.pms.bse.engine.BillingBatchRunResult;
import com.worldpay.pms.spark.core.JSONUtils;
import com.worldpay.pms.spark.core.batch.BatchMetadata;
import com.worldpay.pms.spark.core.jdbc.SqlDb;
import io.vavr.collection.List;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.Arrays;
import lombok.experimental.UtilityClass;
import org.sql2o.Sql2oQuery;
import org.sql2o.data.Column;
import org.sql2o.data.Row;
import org.sql2o.data.Table;

@UtilityClass
public class Db {

  /**
   * tables to clean before running on our billing schema
   */
  static final List<String> OUTPUT_TABLES = List.of(
      "batch_history",
      "outputs_registry",
      "bill_item_error",
      "pending_bill",
      "pending_bill_line",
      "pending_bill_line_calc",
      "pending_bill_line_svc_qty",
      "bill",
      "bill_line",
      "bill_line_calc",
      "bill_line_svc_qty",
      "bill_line_detail",
      "bill_accounting",
      "bill_line_transposition",
      "bill_error",
      "bill_error_detail",
      "bill_tax",
      "bill_tax_detail",
      "bill_tax_fx",
      "bill_price",
      "cm_bill_due_dt",
      "cm_bill_payment_dtl",
      "cm_bill_payment_dtl_snapshot",
      "pending_min_charge",
      "bill_relationship"
  );
  /**
   * views or tables we want to recreate from CISADM/CBE_PCE_OWNER/CBE_CUE_OWNER/CBE_MDU_OWNER on our mock database
   */
  static final List<String> INPUT_TABLES = List.of(
      "vw_billable_item",
      "vw_billable_item_line",
      "vw_billable_item_svc_qty",
      "vw_bill_item_error",
      "vw_misc_bill_item",
      "vw_misc_bill_item_ln",
      "ci_priceitem_char",
      "vw_acct",
      "vw_sub_acct",
      "vw_acct_hier",
      "vw_party",
      "ci_bill_cyc_sch",
      "ci_currency_cd",
      "ci_priceitem_l",
      "ci_sqi_l",
      "cm_price_category",
      "vwm_billing_acct_data",
      "vw_pending_min_charge_s",
      "vw_minimum_charge"
  );

  private static final String QUERY_INSERT_BILLABLE_ITEM = resourceAsString("sql/vw_billable_item/insert-billable-item.sql");
  private static final String QUERY_INSERT_BILLABLE_ITEM_LINE = resourceAsString("sql/vw_billable_item/insert-billable-item-line.sql");
  private static final String QUERY_INSERT_BILLABLE_ITEM_SVC_QTY = resourceAsString(
      "sql/vw_billable_item/insert-billable-item-svc-qty.sql");

  private static final String QUERY_INSERT_MISC_BILLABLE_ITEM = resourceAsString("sql/cm_misc_bill_item/insert-misc-bill-item.sql");
  private static final String QUERY_INSERT_MISC_BILLABLE_ITEM_LINE = resourceAsString("sql/cm_misc_bill_item/insert-misc-bill-item-line.sql");

  private static final String QUERY_INSERT_ACCOUNT_DATA = resourceAsString("sql/vw_acct/insert-account-data.sql");
  private static final String QUERY_INSERT_ACCOUNT = resourceAsString("sql/vw_acct/insert-account.sql");
  private static final String QUERY_INSERT_SUB_ACCOUNT = resourceAsString("sql/vw_acct/insert-sub-account.sql");
  private static final String QUERY_INSERT_ACCOUNT_HIERARCHY = resourceAsString("sql/vw_acct/insert-account-hierarchy.sql");
  private static final String QUERY_INSERT_PARTY = resourceAsString("sql/vw_acct/insert-party.sql");
  private static final String QUERY_INSERT_BILL_CYCLE_SCHEDULE = resourceAsString("sql/vw_acct/insert-bill-cycle-schedule.sql");
  private static final String QUERY_INSERT_PRODUCT_DESCRIPTION = resourceAsString("sql/static_data/insert-product-description.sql");
  private static final String QUERY_INSERT_PRICE_CATEGORY = resourceAsString("sql/static_data/insert-price-category.sql");
  private static final String QUERY_INSERT_PRODUCT_CHARACTERISTIC = resourceAsString("sql/static_data/insert-product-characteristic.sql");
  private static final String QUERY_INSERT_PRODUCT_CHARACTERISTIC_VIEW = resourceAsString("sql/static_data/insert-product-characteristic-view.sql");
  private static final String QUERY_DELETE_PRODUCT_CHARACTERISTIC = resourceAsString("sql/static_data/delete-product-characteristic.sql");
  private static final String QUERY_INSERT_VIEW_MINIMUM_CHARGE_DATA = resourceAsString("sql/static_data/insert-minimum-charge-data.sql");
  private static final String QUERY_INSERT_VIEW_PENDING_MINIMUM_CHARGE = resourceAsString("sql/static_data/insert-pending-min-charge.sql");

  public static void createInputTable(SqlDb sourceDb, SqlDb destDb, String table) {
    List<Column> columns = sourceDb.execQuery(
        "get-columns",
        format("SELECT * FROM %s WHERE 1 = 0", table.toUpperCase()),
        q -> List.ofAll(q.executeAndFetchTable().columns())
    );

    String stmt = format(
        "CREATE TABLE %s (%s)",
        table,
        columns.map(c -> format("%s %s", c.getName(), c.getType())).intersperse(", ").mkString()
    );

    destDb.execQuery(table, stmt, Sql2oQuery::executeUpdate);
  }

  static Table readTable(SqlDb db, String tableName, String where) {
    return db.execQuery(
        tableName,
        format("SELECT * FROM %s WHERE %s", tableName, where == null ? "1=1" : where),
        Sql2oQuery::executeAndFetchTable
    );
  }

  BillingBatchRunResult readLastSuccessfulBatch(SqlDb db) {
    BatchMetadata<BillingBatchRunResult> metadata = JSONUtils.unmarshal(
        readTable(db, "batch_history", "state = 'COMPLETED' ORDER BY created_at DESC FETCH FIRST 1 ROWS ONLY")
            .rows()
            .get(0)
            .getString("metadata"),
        tf -> tf.constructParametrizedType(BatchMetadata.class, BatchMetadata.class, BillingBatchRunResult.class)
    );

    return metadata.getBatchRunResult();
  }

  static void insertBillableItem(Runnable insertBillableItem, Runnable insertBillableItemLine, Runnable... insertBillableItemSvcQty) {
    insertBillableItem.run();
    insertBillableItemLine.run();
    Arrays.stream(insertBillableItemSvcQty).forEach(Runnable::run);
  }

  static void insertAccountDataNoHier(SqlDb db,
      String subAccountId,
      String subAccountType,
      String accountId,
      String accountType,
      String partyId,
      String legalCounterparty,
      String countryCode,
      String currency,
      String businessUnit,
      String processingGroup,
      String billCycleId,
      String status,
      String validFrom,
      String validTo) {
    insertAccountData(db, subAccountId, subAccountType, accountId, partyId, accountType, legalCounterparty, currency, billCycleId, processingGroup, businessUnit, validFrom, validTo);
    insertAccount(db, accountId, partyId, accountType, legalCounterparty, currency, billCycleId, processingGroup, status, validFrom, validTo);
    insertSubAccount(db, accountId, subAccountId, subAccountType, status, validFrom, validTo);
    insertParty(db, partyId, countryCode, businessUnit, validFrom, validTo);
  }

  static void insertAccountDataWithHier(SqlDb db,
      String parentSubAccountId,
      String parentSubAcctType,
      String parentAccountId,
      String parentAccountType,
      String parentPartyId,
      String childSubAccountId,
      String childSubAccountType,
      String childAccountId,
      String childAccountType,
      String childPartyId,
      String legalCounterparty,
      String countryCode,
      String currency,
      String businessUnit,
      String processingGroup,
      String billCycleId,
      String status,
      String validFrom,
      String validTo) {
    insertSubAccount(db, childAccountId, childSubAccountId, childSubAccountType, status, validFrom, validTo);
    insertSubAccount(db, parentAccountId, parentSubAccountId, parentSubAcctType, status, validFrom, validTo);
    insertAccount(db, childAccountId, childPartyId, childAccountType, legalCounterparty, currency, billCycleId, processingGroup, status, validFrom, validTo);
    insertAccount(db, parentAccountId, parentPartyId, parentAccountType, legalCounterparty, currency, billCycleId, processingGroup, status, validFrom, validTo);
    insertParty(db, parentPartyId, countryCode, businessUnit, validFrom, validTo);
    insertParty(db, childPartyId, countryCode, businessUnit, validFrom, validTo);
    insertAccountHierarchy(db, parentAccountId, childAccountId, parentPartyId, childPartyId, status, validFrom, validTo);
    insertAccountData(db, parentSubAccountId, parentSubAcctType, parentAccountId, parentPartyId, parentAccountType, legalCounterparty, currency, billCycleId, processingGroup, businessUnit, validFrom, validTo);
  }

  static void insertStdBillableItem(SqlDb db,
      String billableItemId,
      String subAccountId,
      String legalCounterparty,
      String accruedDate,
      String priceItemCode,
      String priceAssignId,
      String settlementLevelType,
      String settlementGranularity,
      String billingCurrency,
      String currencyFromScheme,
      String fundingCurrency,
      String priceCurrency,
      String transactionCurrency,
      String settlementLevelGranularity,
      String productClass,
      String childProduct,
      int aggregationHash) {
    db.execQuery("insert-billable-item", QUERY_INSERT_BILLABLE_ITEM, (query) ->
        query
            .addParameter("billable_item_id", billableItemId)
            .addParameter("sub_account_id", subAccountId)
            .addParameter("legal_counterparty", legalCounterparty)
            .addParameter("accrued_date", dateFromString(accruedDate))
            .addParameter("priceitem_cd", priceItemCode)
            .addParameter("price_asgn_id", priceAssignId)
            .addParameter("settlement_level_type", settlementLevelType)
            .addParameter("settlement_granularity", settlementGranularity)
            .addParameter("billing_currency", billingCurrency)
            .addParameter("currency_from_scheme", currencyFromScheme)
            .addParameter("funding_currency", fundingCurrency)
            .addParameter("price_currency", priceCurrency)
            .addParameter("txn_currency", transactionCurrency)
            .addParameter("sett_level_granularity", settlementLevelGranularity)
            .addParameter("product_class", productClass)
            .addParameter("child_product", childProduct)
            .addParameter("aggregation_hash", aggregationHash)
            .addParameter("partition_id", 1)
            .executeUpdate());
  }

  static void insertStdBillableItemLine(SqlDb db,
      String billableItemId,
      String distributionId,
      double preciseChargeAmount,
      String characteristicValue,
      String rateType,
      double rate) {
    db.execQuery("insert-billable-item-line", QUERY_INSERT_BILLABLE_ITEM_LINE, (query) ->
        query
            .addParameter("billable_item_id", billableItemId)
            .addParameter("distribution_id", distributionId)
            .addParameter("precise_charge_amount", BigDecimal.valueOf(preciseChargeAmount))
            .addParameter("characteristic_value", characteristicValue)
            .addParameter("rate_type", rateType)
            .addParameter("rate", BigDecimal.valueOf(rate))
            .addParameter("partition_id", 1)
            .executeUpdate());
  }

  static void insertStdBillableItemSvcQty(SqlDb db,
      String billableItemId,
      String sqiCd,
      String rateSchedule,
      double serviceQuantity) {
    db.execQuery("insert-billable-item-svc-qty", QUERY_INSERT_BILLABLE_ITEM_SVC_QTY, (query) ->
        query
            .addParameter("billable_item_id", billableItemId)
            .addParameter("sqi_cd", sqiCd)
            .addParameter("rate_schedule", rateSchedule)
            .addParameter("svc_qty", BigDecimal.valueOf(serviceQuantity))
            .addParameter("partition_id", 1)
            .executeUpdate());
  }

  static void insertMiscBillableItem(SqlDb db,
      String billableItemId,
      String subAccountId,
      String legalCounterparty,
      String accruedDate,
      String adhocBillFlag,
      String currencyCode,
      String productClass,
      String productId,
      double quantity,
      String fastestSettlementIndicator,
      String individualPaymentIndicator,
      String paymentNarrative,
      String releaseReserveIndicator,
      String releaseWAFIndicator,
      String caseIdentifier,
      String debtDate,
      String sourceType,
      String sourceId,
      String status) {
    db.execQuery("insert-misc-billable-item", QUERY_INSERT_MISC_BILLABLE_ITEM, (query) ->
        query
            .addParameter("misc_bill_item_id", billableItemId)
            .addParameter("sub_account_id", subAccountId)
            .addParameter("lcp", legalCounterparty)
            .addParameter("accrued_dt", accruedDate)
            .addParameter("currency_cd", currencyCode)
            .addParameter("product_class", productClass)
            .addParameter("product_id", productId)
            .addParameter("adhoc_bill_flg", adhocBillFlag)
            .addParameter("qty", BigDecimal.valueOf(quantity))
            .addParameter("fastest_payment_flg", fastestSettlementIndicator)
            .addParameter("ind_payment_flg", individualPaymentIndicator)
            .addParameter("pay_narrative", paymentNarrative)
            .addParameter("rel_reserve_flg", releaseReserveIndicator)
            .addParameter("rel_waf_flg", releaseWAFIndicator)
            .addParameter("case_id", caseIdentifier)
            .addParameter("debt_dt", dateFromString(debtDate))
            .addParameter("source_type", sourceType)
            .addParameter("source_id", sourceId)
            .addParameter("status", status)
            .executeUpdate());
  }

  static void insertMiscBillableItemLine(SqlDb db,
      String billableItemId,
      String lineCalculationType,
      double amount,
      double price) {
    db.execQuery("insert-misc-billable-item-line", QUERY_INSERT_MISC_BILLABLE_ITEM_LINE, (query) ->
        query
            .addParameter("bill_item_id", billableItemId)
            .addParameter("line_calc_type", lineCalculationType)
            .addParameter("amount", BigDecimal.valueOf(amount))
            .addParameter("price", BigDecimal.valueOf(price))
            .executeUpdate());
  }

  private static void insertAccountData(SqlDb db,
      String subAccountId,
      String subAccountType,
      String accountId,
      String partyId,
      String accountType,
      String lcp,
      String currency,
      String billCycleId,
      String settlementRegionId,
      String businessUnit,
      String validFrom,
      String validTo) {
    db.execQuery("insert-account-data", QUERY_INSERT_ACCOUNT_DATA, (query) ->
        query
            .addParameter("sub_account_id", subAccountId)
            .addParameter("sub_account_type", subAccountType)
            .addParameter("acct_id", accountId)
            .addParameter("party_id", partyId)
            .addParameter("acct_type", accountType)
            .addParameter("lcp", lcp)
            .addParameter("currency_cd", currency)
            .addParameter("bill_cyc_id", billCycleId)
            .addParameter("settlement_region_id", settlementRegionId)
            .addParameter("business_unit", businessUnit)
            .addParameter("valid_from", dateFromString(validFrom))
            .addParameter("valid_to", dateFromString(validTo))
            .executeUpdate());
  }

  private static void insertAccount(SqlDb db,
      String accountId,
      String partyId,
      String accountType,
      String lcp,
      String currency,
      String billCycleId,
      String settlementRegionId,
      String status,
      String validFrom,
      String validTo) {
    db.execQuery("insert-account", QUERY_INSERT_ACCOUNT, (query) ->
        query
            .addParameter("acct_id", accountId)
            .addParameter("party_id", partyId)
            .addParameter("acct_type", accountType)
            .addParameter("lcp", lcp)
            .addParameter("currency_cd", currency)
            .addParameter("bill_cyc_id", billCycleId)
            .addParameter("settlement_region_id", settlementRegionId)
            .addParameter("status", status)
            .addParameter("valid_from", dateFromString(validFrom))
            .addParameter("valid_to", dateFromString(validTo))
            .executeUpdate());
  }

  static void insertAccountHierarchy(SqlDb db,
      String parentAccountId,
      String childAccountId,
      String parentPartyId,
      String childPartyId,
      String status,
      String validFrom,
      String validTo) {
    db.execQuery("insert-account", QUERY_INSERT_ACCOUNT_HIERARCHY, (query) ->
        query
            .addParameter("parent_acct_id", parentAccountId)
            .addParameter("child_acct_id", childAccountId)
            .addParameter("parent_acct_party_id", parentPartyId)
            .addParameter("child_acct_party_id", childPartyId)
            .addParameter("status", status)
            .addParameter("valid_from", dateFromString(validFrom))
            .addParameter("valid_to", dateFromString(validTo))
            .executeUpdate());
  }

  static void insertSubAccount(SqlDb db,
      String accountId,
      String subAccountId,
      String subAccountType,
      String status,
      String validFrom,
      String validTo) {
    db.execQuery("insert-sub-account", QUERY_INSERT_SUB_ACCOUNT, (query) ->
        query
            .addParameter("sub_acct_id", subAccountId)
            .addParameter("acct_id", accountId)
            .addParameter("sub_acct_type", subAccountType)
            .addParameter("status", status)
            .addParameter("valid_from", dateFromString(validFrom))
            .addParameter("valid_to", dateFromString(validTo))
            .executeUpdate());
  }

  public static void insertParty(SqlDb db,
      String partyId,
      String countryCode,
      String businessUnit,
      String validFrom,
      String validTo) {
    insertParty(db, partyId, countryCode, businessUnit, null, validFrom, validTo);
  }

  public static void insertParty(SqlDb db,
      String partyId,
      String countryCode,
      String businessUnit,
      String merchantTaxRegistration,
      String validFrom,
      String validTo) {
    db.execQuery("insert-party", QUERY_INSERT_PARTY, (query) ->
        query
            .addParameter("party_id", partyId)
            .addParameter("country_cd", countryCode)
            .addParameter("business_unit", businessUnit)
            .addParameter("merch_tax_reg", merchantTaxRegistration)
            .addParameter("valid_from", dateFromString(validFrom))
            .addParameter("valid_to", dateFromString(validTo))
            .executeUpdate());
  }

  static void insertBillCycleSchedule(SqlDb db,
      String billCycleCode,
      String windowStartDate,
      String windowEndDate) {
    db.execQuery("insert-bill-cycle-schedule", QUERY_INSERT_BILL_CYCLE_SCHEDULE, (query) ->
        query
            .addParameter("bill_cyc_cd", billCycleCode)
            .addParameter("win_start_dt", dateFromString(windowStartDate))
            .addParameter("win_end_dt", dateFromString(windowEndDate))
            .executeUpdate());
  }

  static void insertProductDescription(SqlDb db,
      String productCode,
      String description) {
    db.execQuery("insert-product-description", QUERY_INSERT_PRODUCT_DESCRIPTION, (query) ->
        query
            .addParameter("priceitem_cd", productCode)
            .addParameter("descr", description)
            .executeUpdate());
  }

  static void insertPriceCategory(SqlDb db,
      String priceCtgId,
      String description) {
    db.execQuery("insert-price-category", QUERY_INSERT_PRICE_CATEGORY, (query) ->
        query
            .addParameter("price_ctg_id", priceCtgId)
            .addParameter("descr", description)
            .executeUpdate());
  }
  public static void insertProductCharacteristic(SqlDb db,
      String productCode,
      String characteristicType,
      String characteristicValue) {
    insertProductCharacteristic(db, productCode, characteristicType, characteristicValue, QUERY_INSERT_PRODUCT_CHARACTERISTIC);
  }

  public static void insertProductCharacteristicView(SqlDb db,
      String productCode,
      String characteristicType,
      String characteristicValue) {
    insertProductCharacteristic(db, productCode, characteristicType, characteristicValue, QUERY_INSERT_PRODUCT_CHARACTERISTIC_VIEW);
  }

  private static void insertProductCharacteristic(SqlDb db,
      String productCode,
      String characteristicType,
      String characteristicValue, String query) {
    db.execQuery("insert-product-characteristic", query, (q) ->
        q
            .addParameter("priceitem_cd", productCode)
            .addParameter("char_type_cd", characteristicType)
            .addParameter("char_val", characteristicValue)
            .executeUpdate());
  }

  public static void deleteProductCharacteristic(SqlDb db,
      String productCode,
      String characteristicType,
      String characteristicValue) {
    db.execQuery("delete-product-characteristic", QUERY_DELETE_PRODUCT_CHARACTERISTIC, (query) ->
        query
            .addParameter("priceitem_cd", productCode)
            .addParameter("char_type_cd", characteristicType)
            .addParameter("char_val", characteristicValue)
            .executeUpdate());
  }

  public static void insertMinimumCharge(
      SqlDb db,
      String price_assign_id,
      String lcp,
      String party_id,
      String min_chg_period,
      String rate_tp,
      double rate,
      String start_dt,
      String end_dt,
      String price_status_flag,
      String currency) {
    db.execQuery("insert-minimum-charge-data", QUERY_INSERT_VIEW_MINIMUM_CHARGE_DATA, (query) ->
        query.addParameter("price_assign_id",price_assign_id)
             .addParameter("lcp",lcp)
             .addParameter("party_id",party_id)
             .addParameter("min_chg_period",min_chg_period)
             .addParameter("rate_tp",rate_tp)
             .addParameter("rate",rate)
             .addParameter("start_dt",dateFromString(start_dt))
             .addParameter("end_dt",dateFromString(end_dt))
             .addParameter("price_status_flag",price_status_flag)
             .addParameter("currency",currency)
             .executeUpdate());
  }

  public static void insertPendingMinimumCharge(
      SqlDb db,
      String lcp,
      String party_id,
      String currency) {
    db.execQuery("insert-minimum-charge-data", QUERY_INSERT_VIEW_PENDING_MINIMUM_CHARGE, (query) ->
        query.addParameter("lcp",lcp)
            .addParameter("txn_party_id",party_id)
            .addParameter("currency",currency)
            .executeUpdate());
  }

  static Row getBillLineDetail(SqlDb db, String billableItemId) {
    Table table = readTable(db, "bill_line_detail",
        String.format("bill_item_id = '%s' ORDER BY cre_dttm DESC FETCH FIRST 1 ROWS ONLY", billableItemId));
    if (table.rows().size() != 1) {
      fail("0 bill line details found for billable item id = " + billableItemId);
    }
    return table.rows().get(0);
  }

  static Row getPendingBill(SqlDb db, String billableItemId) {
    Row billLineDetail = getBillLineDetail(db, billableItemId);
    String billId = billLineDetail.getString("bill_id");
    Table table = readTable(db, "pending_bill",
        String.format("bill_id = '%s' ORDER BY cre_dttm DESC FETCH FIRST 1 ROWS ONLY", billId));
    if (table.rows().size() != 1) {
      fail("0 pending bills found for billable item id = " + billableItemId);
    }
    return table.rows().get(0);
  }

  private static Date dateFromString(String date) {
    return date == null ? null : Date.valueOf(date);
  }
}
