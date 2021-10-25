CREATE OR REPLACE VIEW vw_payment_request_granularity
AS
SELECT b.bill_id                                    AS bill_id,
       '1'                                          AS line_id,
       TRIM(
         REGEXP_SUBSTR(
           REGEXP_SUBSTR(b.granularity_key_val, '[^,]+', 1, lvl),
           '[^\|]+', 1, 1
         )
       )                                            AS granularities_type,
       TRIM(
         REGEXP_SUBSTR(
           REGEXP_SUBSTR(b.granularity_key_val, '[^,]+', 1, lvl),
           '[^\|]+', 1, 2
         )
       )                                            AS reference_val,
       1                                            AS pay_detail_id,
       b.cre_dttm                                   AS upload_dttm,
       'Y'                                          AS extract_flg,
       NULL                                         AS extract_dttm,
       'MPG'                                        AS mpg_type
FROM
  vw_bill b
CROSS APPLY (
  SELECT level as lvl
  FROM dual
  CONNECT BY level <= (REGEXP_COUNT(b.granularity_key_val, ',') + 1)
)
WHERE b.granularity_key_val IS NOT NULL
  AND b.prev_bill_id IS NULL AND (b.debt_mig_type IS NULL OR b.debt_mig_type <> 'MIG_DEBT');