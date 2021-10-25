CREATE OR REPLACE VIEW vw_acct
AS
SELECT  acct_id,
        party_id,
        acct_type,
        lcp,
        currency_cd,
        status,
        bill_cyc_id,
        settlement_region_id,
        valid_from,
        valid_to,
        cre_dttm,
        last_upd_dttm
FROM acct p
WHERE EXISTS(
              SELECT 1
              FROM vw_batch_history r
              WHERE r.batch_code = p.batch_code
                AND r.attempt = p.batch_attempt
          );