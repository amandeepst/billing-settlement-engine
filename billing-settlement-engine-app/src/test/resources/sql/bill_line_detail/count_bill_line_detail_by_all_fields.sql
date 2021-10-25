SELECT COUNT(1)
FROM bill_line_detail
WHERE bill_item_id   = :bill_item_id
  AND bill_item_hash = :bill_item_hash
  AND bill_id        = :bill_id
  AND bill_ln_id     = :bill_ln_id
  AND cre_dttm       IS NOT NULL
  AND batch_code     = :batch_code
  AND batch_attempt  = :batch_attempt
  AND partition_id   IS NOT NULL
  AND ilm_dt         = :ilm_dt
  AND ilm_arch_sw    = :ilm_arch_sw
  AND partition      IS NOT NULL