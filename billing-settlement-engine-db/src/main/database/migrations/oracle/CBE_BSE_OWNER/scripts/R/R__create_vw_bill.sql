CREATE OR REPLACE VIEW vw_bill
AS
SELECT bill_id,
       bill_nbr,
       party_id,
       bill_sub_acct_id,
       tariff_type,
       template_type,
       lcp,
       acct_type,
       account_id,
       business_unit,
       bill_dt,
       bill_cyc_id,
       start_dt,
       end_dt,
       currency_cd,
       bill_amt,
       bill_ref,
       status,
       adhoc_bill_flg,
       sett_sub_lvl_type,
       sett_sub_lvl_val,
       granularity,
       granularity_key_val,
       rel_waf_flg,
       rel_reserve_flg,
       fastest_pay_route,
       case_id,
       individual_bill,
       manual_narrative,
       settlement_region_id,
       prev_bill_id,
       debt_dt,
       debt_mig_type,
       merch_tax_reg,
       wp_tax_reg,
       tax_type,
       tax_authority,
       bill_map_id,
       cre_dttm,
       partition AS partition_id,
       ilm_dt,
       ilm_arch_sw
FROM bill b
WHERE EXISTS(
              SELECT 1
              FROM outputs_registry r
              WHERE r.batch_code = b.batch_code
                AND r.batch_attempt = b.batch_attempt
                AND r.dataset_id = 'BILL'
                AND r.visible = 'Y'
          );