SELECT COUNT(1)
FROM post_bill_adj
WHERE post_bill_adj_id = :post_bill_adj_id
  AND post_bill_adj_type = :post_bill_adj_type
  AND post_bill_adj_descr = :post_bill_adj_descr
  AND bill_id = :bill_id
  AND amount = :amount
  AND currency_cd = :currency_cd
  AND bill_sub_acct_id = :bill_sub_acct_id
  AND rel_sub_acct_id = :rel_sub_acct_id
  AND party_id = :party_id
  AND bill_ref = :bill_ref
  AND granularity = :granularity
  AND acct_type = :acct_type
  AND batch_code = :batch_code
  AND batch_attempt = :batch_attempt
  AND partition_id = :partition_id
  AND ilm_dt = :ilm_dt
  AND ilm_arch_sw = :ilm_arch_sw
  AND source_id IS NULL
  AND bill_dt = :bill_dt