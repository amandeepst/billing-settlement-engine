CREATE OR REPLACE VIEW vw_acct_hier
AS
SELECT  parent_acct_party_id  	as parent_acct_party_id,
        parent_acct_id		  	  as parent_acct_id,
        child_acct_party_id		  as child_acct_party_id,
        child_acct_id			      as child_acct_id,
        status					        as status,
        valid_from				      as valid_from,
        valid_to				        as valid_to,
        cre_dttm				        as cre_dttm,
        last_upd_dttm			      as last_upd_dttm
FROM acct_hier p
WHERE EXISTS(
              SELECT 1
              FROM vw_batch_history r
              WHERE r.batch_code = p.batch_code
                AND r.attempt = p.batch_attempt
          );