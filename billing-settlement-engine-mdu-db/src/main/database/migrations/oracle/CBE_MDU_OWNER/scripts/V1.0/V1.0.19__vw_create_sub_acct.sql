CREATE OR REPLACE VIEW vw_sub_acct
AS
SELECT  sub_acct_id     as sub_acct_id,
        acct_id			    as acct_id,
        sub_acct_type	  as sub_acct_type,
        status			    as status,
        valid_from		  as valid_from,
        valid_to		    as valid_to,
        cre_dttm		    as cre_dttm,
        last_upd_dttm	  as last_upd_dttm
FROM sub_acct p
WHERE EXISTS(
              SELECT 1
              FROM vw_batch_history r
              WHERE r.batch_code = p.batch_code
                AND r.attempt = p.batch_attempt
          );