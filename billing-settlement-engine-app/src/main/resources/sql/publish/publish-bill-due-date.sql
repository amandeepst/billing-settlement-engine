INSERT /*+ :insert-hints */ INTO cm_bill_due_dt (
    bank_entry_event_id,
    bill_id,
    due_dt,
    is_merch_balanced,
    upload_dttm,
    status_upd_dttm,
    pay_dt,
    line_id,
    banking_entry_status
)
WITH price AS (
  SELECT bill_id, MAX(source_id) AS source_id
  FROM bill_price
  WHERE batch_code = :batch_code
    AND batch_attempt = :batch_attempt
    AND ilm_dt = :ilm_dt
  GROUP BY bill_id
)
SELECT /*+ :select-hints */
    NVL(p.source_id, '0'),
    b.bill_id,
    b.debt_dt,
    'N',
    SYSDATE,
    null,
    b.cre_dttm,
    '1',
    'DEBT_MIGRATION'
FROM bill b LEFT JOIN price p ON b.bill_id = p.bill_id
WHERE batch_code = :batch_code
  AND batch_attempt = :batch_attempt
  AND ilm_dt = :ilm_dt
  AND debt_mig_type = 'MIG_DEBT'