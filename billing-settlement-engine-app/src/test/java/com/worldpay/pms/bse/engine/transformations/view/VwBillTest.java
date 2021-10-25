package com.worldpay.pms.bse.engine.transformations.view;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.worldpay.pms.bse.engine.transformations.model.billprice.BillPriceRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillLineDetailRow;
import com.worldpay.pms.bse.engine.transformations.model.completebill.BillRow;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;

public class VwBillTest extends SimpleViewTest {

  private static final String QUERY_INSERT_BILL = resourceAsString("sql/bill/insert_bill.sql");
  private static final String QUERY_COUNT_BILL_BY_ALL_FIELDS = resourceAsString("sql/bill/count_vw_bill_by_all_fields.sql");

  static final BillRow BILL = createBill();

  private static BillRow createBill() {
    return new BillRow("billId", "partyId", "billSubAccountId",
        "tariffType", "templateType", "00001", "FUND", "accountId", "businessUnit", Date.valueOf(LocalDate.now()),
        "MONTHLY", Date.valueOf("2021-01-01"), Date.valueOf("2021-01-31"), "EUR", BigDecimal.TEN, "billRef", "COMPLETE",
        "N", "settSubLevelType", "settSubLevelValue", "granularity", "granularityKeyVal",
        "N", "N", "N", "N", "N", "N", "procGroup1", "prevBillId1",
        Date.valueOf("2020-01-01"), "T1", "m_tax_reg_1", "wp_tax_reg_1", "S", "HMRC", null, null, null, null, null, null,
        new BillLineDetailRow[0], new BillPriceRow[0], null, (short) 0);
  }

  @Override
  protected String getTableName() {
    return "bill";
  }

  @Override
  protected void insertEntry(String id, String batchHistoryCode, int batchAttempt) {
    sqlDb.execQuery("insert_bill", QUERY_INSERT_BILL, (query) ->
        query
            .addParameter("bill_id", id)
            .addParameter("bill_nbr", "billNumber")
            .addParameter("party_id", BILL.getPartyId())
            .addParameter("bill_sub_acct_id", BILL.getBillSubAccountId())
            .addParameter("tariff_type", BILL.getTariffType())
            .addParameter("template_type", BILL.getTemplateType())
            .addParameter("lcp", BILL.getLegalCounterparty())
            .addParameter("acct_type", BILL.getAccountType())
            .addParameter("account_id", BILL.getAccountId())
            .addParameter("business_unit", BILL.getBusinessUnit())
            .addParameter("bill_dt", BILL.getBillDate())
            .addParameter("bill_cyc_id", BILL.getBillCycleId())
            .addParameter("start_dt", BILL.getStartDate())
            .addParameter("end_dt", BILL.getEndDate())
            .addParameter("currency_cd", BILL.getCurrencyCode())
            .addParameter("bill_amt", BILL.getBillAmount())
            .addParameter("bill_ref", BILL.getBillReference())
            .addParameter("status", BILL.getStatus())
            .addParameter("adhoc_bill_flg", BILL.getAdhocBillFlag())
            .addParameter("sett_sub_lvl_type", BILL.getSettlementSubLevelType())
            .addParameter("sett_sub_lvl_val", BILL.getSettlementSubLevelValue())
            .addParameter("granularity", BILL.getGranularity())
            .addParameter("granularity_key_val", BILL.getGranularityKeyValue())
            .addParameter("rel_waf_flg", BILL.getReleaseReserveIndicator())
            .addParameter("rel_reserve_flg", BILL.getReleaseReserveIndicator())
            .addParameter("fastest_pay_route", BILL.getFastestPaymentRouteIndicator())
            .addParameter("case_id", BILL.getCaseId())
            .addParameter("individual_bill", BILL.getIndividualBillIndicator())
            .addParameter("manual_narrative", BILL.getManualNarrative())
            .addParameter("settlement_region_id", BILL.getProcessingGroup())
            .addParameter("prev_bill_id", BILL.getPreviousBillId())
            .addParameter("debt_dt", BILL.getDebtDate())
            .addParameter("debt_mig_type", BILL.getDebtMigrationType())
            .addParameter("merch_tax_reg", BILL.getMerchantTaxRegistrationNumber())
            .addParameter("wp_tax_reg", BILL.getWorldpayTaxRegistrationNumber())
            .addParameter("tax_type", BILL.getTaxType())
            .addParameter("tax_authority", BILL.getTaxAuthority())
            .addParameter("batch_code", batchHistoryCode)
            .addParameter("batch_attempt", batchAttempt)
            .addParameter("partition_id", 1)
            .addParameter("ilm_dt", GENERIC_TIMESTAMP)
            .executeUpdate());
  }

  @Override
  protected long countVwEntryByAllFields(String id) {
    return sqlDb.execQuery("count_bill", QUERY_COUNT_BILL_BY_ALL_FIELDS, (query) ->
        query
            .addParameter("bill_id", id)
            .addParameter("bill_nbr", "billNumber")
            .addParameter("party_id", BILL.getPartyId())
            .addParameter("bill_sub_acct_id", BILL.getBillSubAccountId())
            .addParameter("tariff_type", BILL.getTariffType())
            .addParameter("template_type", BILL.getTemplateType())
            .addParameter("lcp", BILL.getLegalCounterparty())
            .addParameter("acct_type", BILL.getAccountType())
            .addParameter("account_id", BILL.getAccountId())
            .addParameter("business_unit", BILL.getBusinessUnit())
            .addParameter("bill_dt", BILL.getBillDate())
            .addParameter("bill_cyc_id", BILL.getBillCycleId())
            .addParameter("start_dt", BILL.getStartDate())
            .addParameter("end_dt", BILL.getEndDate())
            .addParameter("currency_cd", BILL.getCurrencyCode())
            .addParameter("bill_amt", BILL.getBillAmount())
            .addParameter("bill_ref", BILL.getBillReference())
            .addParameter("status", BILL.getStatus())
            .addParameter("adhoc_bill_flg", BILL.getAdhocBillFlag())
            .addParameter("sett_sub_lvl_type", BILL.getSettlementSubLevelType())
            .addParameter("sett_sub_lvl_val", BILL.getSettlementSubLevelValue())
            .addParameter("granularity", BILL.getGranularity())
            .addParameter("granularity_key_val", BILL.getGranularityKeyValue())
            .addParameter("rel_waf_flg", BILL.getReleaseWafIndicator())
            .addParameter("rel_reserve_flg", BILL.getReleaseReserveIndicator())
            .addParameter("fastest_pay_route", BILL.getFastestPaymentRouteIndicator())
            .addParameter("case_id", BILL.getCaseId())
            .addParameter("individual_bill", BILL.getIndividualBillIndicator())
            .addParameter("manual_narrative", BILL.getManualNarrative())
            .addParameter("settlement_region_id", BILL.getProcessingGroup())
            .addParameter("prev_bill_id", BILL.getPreviousBillId())
            .addParameter("debt_dt", BILL.getDebtDate())
            .addParameter("debt_mig_type", BILL.getDebtMigrationType())
            .addParameter("merch_tax_reg", BILL.getMerchantTaxRegistrationNumber())
            .addParameter("wp_tax_reg", BILL.getWorldpayTaxRegistrationNumber())
            .addParameter("tax_type", BILL.getTaxType())
            .addParameter("tax_authority", BILL.getTaxAuthority())
            .executeScalar(Long.TYPE));
  }
}
