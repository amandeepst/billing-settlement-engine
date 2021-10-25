CREATE OR REPLACE VIEW vw_withhold_funds
AS
SELECT  acct_id					      as acct_id,
        withhold_fund_type		as withhold_fund_type,
        withhold_fund_target	as withhold_fund_target,
        withhold_fund_prcnt		as withhold_fund_prcnt,
        valid_from				    as valid_from,
        valid_to				      as valid_to,
        cre_dttm				      as cre_dttm,
        last_upd_dttm			    as last_upd_dttm
FROM withhold_funds p
WHERE EXISTS(
              SELECT 1
              FROM vw_batch_history r
              WHERE r.batch_code = p.batch_code
                AND r.attempt = p.batch_attempt
          );