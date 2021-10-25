CREATE OR REPLACE VIEW vw_bill_error_detail
AS
SELECT bill_id,
       bill_ln_id,
       code         AS error_cd,
       reason       AS error_info
FROM bill_error_detail b
WHERE EXISTS(
              SELECT 1
              FROM outputs_registry r
              WHERE r.batch_code = b.batch_code
                AND r.batch_attempt = b.batch_attempt
                AND r.dataset_id = 'BILL_ERROR'
                AND r.visible = 'Y'
          );