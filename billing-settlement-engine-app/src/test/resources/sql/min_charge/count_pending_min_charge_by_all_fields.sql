SELECT COUNT(1)
FROM pending_min_charge
WHERE bill_party_id = :bill_party_id
  AND legal_counterparty = :legal_counterparty
  AND txn_party_id = :txn_party_id
  AND min_chg_start_dt = :min_chg_start_dt
  AND min_chg_end_dt = :min_chg_end_dt
  AND min_chg_type = :min_chg_type
  AND applicable_charges = :applicable_charges
  AND currency = :currency
  AND bill_dt = :bill_dt
  AND cre_dttm IS NOT NULL
  AND batch_code = :batch_code
  AND batch_attempt = :batch_attempt
  AND partition_id IS NOT NULL
  AND ilm_dt = :ilm_dt
  AND ilm_arch_sw = :ilm_arch_sw