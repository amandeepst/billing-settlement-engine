CREATE OR REPLACE VIEW vw_post_bill_adj_accounting
AS
SELECT  party_id,
        acct_id,
        acct_type,
        sub_acct_id,
        sub_acct_type,
        currency_cd,
        bill_amt,
        bill_id,
        post_bill_adj_id,
        amount,
        lcp,
        business_unit,
        class,
        type,
        bill_dt,
        cre_dttm,
        batch_code,
        batch_attempt,
        ilm_dt,
        ilm_arch_sw,
        partition AS partition_id
FROM post_bill_adj_accounting b
WHERE EXISTS(
             SELECT 1
             FROM outputs_registry_post_billing r
             WHERE r.batch_code = b.batch_code
             AND r.batch_attempt = b.batch_attempt
             AND r.dataset_id = 'POST_BILL_ADJ_ACCOUNTING'
             AND r.visible = 'Y'
            );