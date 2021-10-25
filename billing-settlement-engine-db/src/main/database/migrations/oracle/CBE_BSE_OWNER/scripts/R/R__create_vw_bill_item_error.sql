CREATE OR REPLACE VIEW vw_bill_item_error
AS
SELECT
    bill_item_id,
    reason,
    first_failure_on,
    retry_count,
    cre_dttm,
    ilm_dt,
    billable_item_ilm_dt
FROM bill_item_error b
WHERE retry_count <> 999 -- filtering ignored txns
  AND status = 'EROR'
  AND EXISTS(
        SELECT 1
        FROM outputs_registry r
        WHERE r.ilm_dt = b.ilm_dt
          AND r.batch_code = b.batch_code
          AND r.batch_attempt = b.batch_attempt
          AND r.dataset_id = 'BILL_ITEM_ERROR'
          AND r.visible = 'Y'
    );
