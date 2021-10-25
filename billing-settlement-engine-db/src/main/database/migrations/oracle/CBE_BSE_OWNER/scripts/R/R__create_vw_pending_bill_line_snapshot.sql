CREATE OR REPLACE VIEW vw_pending_bill_line_s
AS
WITH last_run AS (
  SELECT ilm_dt, batch_code, batch_attempt FROM outputs_registry
  WHERE dataset_id = 'PENDING_BILL' AND visible = 'Y'
  ORDER BY ilm_dt DESC
  FETCH FIRST 1 ROWS ONLY
)
SELECT b.bill_ln_id,
       b.bill_id,
       b.bill_line_party_id,
       b.product_class,
       b.product_id,
       b.price_ccy,
       b.fund_ccy,
       b.fund_amt,
       b.txn_ccy,
       b.txn_amt,
       b.qty,
       b.price_ln_id,
       b.merchant_code,
       b.batch_code,
       b.batch_attempt,
       b.ilm_dt,
       b.ilm_arch_sw,
       b.partition AS partition_id
FROM pending_bill_line b, last_run lr
WHERE b.ilm_dt        = lr.ilm_dt
  AND b.batch_code    = lr.batch_code
  AND b.batch_attempt = lr.batch_attempt;