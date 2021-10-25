CREATE OR REPLACE VIEW vw_pending_bill_line_svc_qty
AS
SELECT bill_id,
       bill_ln_id,
       svc_qty_cd,
       svc_qty,
       batch_code,
       batch_attempt,
       ilm_dt,
       ilm_arch_sw,
       partition AS partition_id
FROM pending_bill_line_svc_qty b
WHERE EXISTS(
              SELECT 1
              FROM outputs_registry r
              WHERE r.batch_code = b.batch_code
                AND r.batch_attempt = b.batch_attempt
                AND r.dataset_id = 'PENDING_BILL'
                AND r.visible = 'Y'
          );