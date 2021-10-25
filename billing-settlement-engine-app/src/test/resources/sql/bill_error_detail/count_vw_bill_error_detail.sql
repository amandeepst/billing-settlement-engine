SELECT COUNT(1)
FROM vw_bill_error_detail
WHERE bill_id = :bill_id
  AND bill_ln_id = :bill_ln_id
  AND error_cd = :error_cd
  AND error_info = :error_info
























