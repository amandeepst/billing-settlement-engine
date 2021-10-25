CREATE OR REPLACE VIEW vw_pending_bill_line
AS
SELECT bill_ln_id,
       bill_id,
       bill_line_party_id,
       product_class,
       product_id,
       price_ccy,
       fund_ccy,
       fund_amt,
       txn_ccy,
       txn_amt,
       qty,
       price_ln_id,
       merchant_code,
       batch_code,
       batch_attempt,
       ilm_dt,
       ilm_arch_sw,
       partition AS partition_id
FROM pending_bill_line b
WHERE EXISTS(
              SELECT 1
              FROM outputs_registry r
              WHERE r.batch_code = b.batch_code
                AND r.batch_attempt = b.batch_attempt
                AND r.dataset_id = 'PENDING_BILL'
                AND r.visible = 'Y'
          );