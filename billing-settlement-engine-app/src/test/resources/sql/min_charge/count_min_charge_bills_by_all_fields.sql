SELECT COUNT(1)
FROM min_charge_bills
WHERE bill_party_id = :bill_party_id
  AND legal_counterparty = :legal_counterparty
  AND currency = :currency
  AND logical_date = :logical_date
  AND batch_code = :batch_code
  AND batch_attempt = :batch_attempt
  AND partition_id IS NOT NULL
  AND ilm_dt = :ilm_dt
  AND ilm_arch_sw = :ilm_arch_sw