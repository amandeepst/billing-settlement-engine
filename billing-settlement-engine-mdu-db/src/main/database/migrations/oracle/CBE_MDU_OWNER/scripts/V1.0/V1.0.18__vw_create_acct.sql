CREATE OR REPLACE VIEW vw_acct
AS
SELECT  acct_id         as acct_id,
        party_id        as party_id,
        acct_type       as acct_type,
        lcp             as lcp,
        currency_cd     as currency_cd,
        status          as status,
        bill_cyc_id     as bill_cyc_id,
        processing_grp  as processing_grp,
        valid_from      as valid_from,
        valid_to        as valid_to,
        cre_dttm        as cre_dttm,
        last_upd_dttm   as last_upd_dttm
FROM acct p
WHERE EXISTS(
              SELECT 1
              FROM vw_batch_history r
              WHERE r.batch_code = p.batch_code
                AND r.attempt = p.batch_attempt
          );