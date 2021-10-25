CREATE OR REPLACE VIEW vw_post_bill_adj
AS
SELECT post_bill_adj_id,
       post_bill_adj_type,
       post_bill_adj_descr,
       source_id,
       bill_id,
       amount,
       currency_cd,
       bill_sub_acct_id,
       rel_sub_acct_id,
       party_id,
       bill_ref,
       granularity,
       acct_type,
       bill_dt,
       source_key,
       non_event_id,
       cre_dttm,
       batch_code,
       batch_attempt,
       ilm_dt,
       ilm_arch_sw
FROM post_bill_adj b
WHERE EXISTS(
              SELECT 1
              FROM outputs_registry_post_billing r
              WHERE r.batch_code = b.batch_code
                AND r.batch_attempt = b.batch_attempt
                AND r.dataset_id = 'POST_BILL_ADJ'
                AND r.visible = 'Y'
          );