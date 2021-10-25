CREATE OR REPLACE VIEW vw_pending_min_charge_s
AS
WITH last_run AS (
  SELECT ilm_dt, batch_code, batch_attempt FROM outputs_registry
  WHERE dataset_id = 'MIN_CHARGE' AND visible = 'Y'
  ORDER BY ilm_dt DESC
  FETCH FIRST 1 ROWS ONLY
)
SELECT m.bill_party_id,
       m.legal_counterparty,
       m.txn_party_id,
       m.min_chg_start_dt,
       m.min_chg_end_dt,
       m.min_chg_type,
       m.applicable_charges,
       currency,
       m.bill_dt,
       m.ilm_dt
FROM pending_min_charge m, last_run lr
WHERE m.ilm_dt        = lr.ilm_dt
  AND m.batch_code    = lr.batch_code
  AND m.batch_attempt = lr.batch_attempt;