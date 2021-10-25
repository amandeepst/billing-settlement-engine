CREATE OR REPLACE VIEW vw_bill_error
AS
SELECT bill_id,
       bill_dt,
       cre_dttm AS error_dt,
       fix_dt,
       batch_code
FROM bill_error b
WHERE EXISTS(
              SELECT 1
              FROM outputs_registry r
              WHERE r.batch_code = b.batch_code
                AND r.batch_attempt = b.batch_attempt
                AND r.dataset_id = 'BILL_ERROR'
                AND r.visible = 'Y'
          );