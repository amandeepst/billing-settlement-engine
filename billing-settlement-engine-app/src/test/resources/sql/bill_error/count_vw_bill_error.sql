SELECT COUNT(1)
FROM vw_bill_error
WHERE bill_id = :bill_id
  AND bill_dt = :bill_dt
  AND error_dt IS NOT NULL
  AND fix_dt IS NULL
  AND batch_code IS NOT NULL
