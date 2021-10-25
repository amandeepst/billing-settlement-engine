CREATE OR REPLACE VIEW vw_pending_bill_line_svc_qty_s
AS
WITH last_run AS (
  SELECT ilm_dt, batch_code, batch_attempt FROM outputs_registry
  WHERE dataset_id = 'PENDING_BILL' AND visible = 'Y'
  ORDER BY ilm_dt DESC
  FETCH FIRST 1 ROWS ONLY
)
SELECT b.bill_id,
       b.bill_ln_id,
       b.svc_qty_cd,
       b.svc_qty,
       b.batch_code,
       b.batch_attempt,
       b.ilm_dt,
       b.ilm_arch_sw,
       b.partition AS partition_id
FROM pending_bill_line_svc_qty b, last_run lr
WHERE b.ilm_dt        = lr.ilm_dt
  AND b.batch_code    = lr.batch_code
  AND b.batch_attempt = lr.batch_attempt;