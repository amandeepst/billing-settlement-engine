CREATE OR REPLACE VIEW vw_invoice_data_adj
AS
SELECT  post_bill_adj_id        AS adj_id,
        post_bill_adj_type      AS adj_type_cd,
        post_bill_adj_descr     AS descr,
        bill_id,
        amount                  AS adj_amt,
        currency_cd,
        cre_dttm                AS upload_dttm,
        'Y   '                  AS extract_flg,
        null                    AS extract_dttm,
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