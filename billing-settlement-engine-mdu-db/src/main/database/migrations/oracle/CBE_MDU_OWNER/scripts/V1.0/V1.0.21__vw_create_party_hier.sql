CREATE OR REPLACE VIEW vw_party_hier
AS
SELECT  parent_party_id			as parent_party_id,
        child_party_id			as child_party_id,
        party_hier_type			as party_hier_type,
        status					    as status,
        valid_from				  as valid_from,
        valid_to				    as valid_to,
        cre_dttm				    as cre_dttm,
        last_upd_dttm			  as last_upd_dttm
FROM party_hier p
WHERE EXISTS(
              SELECT 1
              FROM vw_batch_history r
              WHERE r.batch_code = p.batch_code
                AND r.attempt = p.batch_attempt
          );