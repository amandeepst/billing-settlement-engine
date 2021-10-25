CREATE OR REPLACE VIEW vw_pending_min_charge
AS
SELECT   bill_party_id,
         legal_counterparty,
         txn_party_id,
         min_chg_start_dt,
         min_chg_end_dt,
         min_chg_type,
         applicable_charges,
         currency,
         bill_dt,
         ilm_dt
FROM pending_min_charge m
WHERE EXISTS(
              SELECT 1
              FROM outputs_registry r
              WHERE r.batch_code = m.batch_code
                AND r.batch_attempt = m.batch_attempt
                AND r.dataset_id = 'MIN_CHARGE'
                AND r.visible = 'Y'
          );