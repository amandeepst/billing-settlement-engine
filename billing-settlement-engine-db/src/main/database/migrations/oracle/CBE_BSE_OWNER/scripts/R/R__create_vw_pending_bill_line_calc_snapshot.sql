CREATE OR REPLACE VIEW vw_pending_bill_line_calc_s
AS
WITH last_run AS (
  SELECT ilm_dt, batch_code, batch_attempt FROM outputs_registry
  WHERE dataset_id = 'PENDING_BILL' AND visible = 'Y'
  ORDER BY ilm_dt DESC
  FETCH FIRST 1 ROWS ONLY
)
SELECT b.bill_line_calc_id,
       b.bill_ln_id,
       b.bill_id,
       b.calc_ln_class,
       b.calc_ln_type,
       b.amount,
       b.include_on_bill,
       b.rate_type,
       b.rate_val,
       b.batch_code,
       b.batch_attempt,
       b.ilm_dt,
       b.ilm_arch_sw,
       b.partition AS partition_id
FROM pending_bill_line_calc b, last_run lr
WHERE b.ilm_dt        = lr.ilm_dt
  AND b.batch_code    = lr.batch_code
  AND b.batch_attempt = lr.batch_attempt;