CREATE OR REPLACE VIEW vw_min_charge_bill
AS
SELECT   bill_party_id,
         legal_counterparty,
         currency,
         logical_date,
         ilm_dt
FROM minimum_charge_bill m
WHERE EXISTS(
              SELECT 1
              FROM outputs_registry r
              WHERE r.batch_code = m.batch_code
                AND r.batch_attempt = m.batch_attempt
                AND r.dataset_id = 'BILL'
                AND r.visible = 'Y'
          );