CREATE OR REPLACE VIEW vw_pending_bill_s
AS
WITH last_run AS (
  SELECT ilm_dt, batch_code, batch_attempt FROM outputs_registry
  WHERE dataset_id = 'PENDING_BILL' AND visible = 'Y'
  ORDER BY ilm_dt DESC
  FETCH FIRST 1 ROWS ONLY
)
SELECT b.bill_id,
       b.party_id,
       b.lcp,
       b.acct_id,
       b.bill_sub_acct_id,
       b.acct_type,
       b.business_unit,
       b.bill_cyc_id,
       b.start_dt,
       b.end_dt,
       b.currency_cd,
       b.bill_ref,
       b.adhoc_bill,
       b.sett_sub_lvl_type,
       b.sett_sub_lvl_val,
       b.granularity,
       b.granularity_key_val,
       b.debt_dt,
       b.debt_mig_type,
       b.overpayment_flg,
       b.rel_waf_flg,
       b.rel_reserve_flg,
       b.fastest_pay_route,
       b.case_id,
       b.individual_bill,
       b.manual_narrative,
       b.miscalculation_flag,
       b.batch_code,
       b.batch_attempt,
       b.ilm_dt,
       b.ilm_arch_sw,
       b.first_failure_on,
       b.retry_count,
       b.partition AS partition_id
FROM pending_bill b, last_run lr
WHERE b.ilm_dt        = lr.ilm_dt
  AND b.batch_code    = lr.batch_code
  AND b.batch_attempt = lr.batch_attempt;