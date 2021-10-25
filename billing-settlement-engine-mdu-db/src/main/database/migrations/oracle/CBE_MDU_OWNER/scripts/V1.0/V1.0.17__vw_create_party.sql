CREATE OR REPLACE VIEW vw_party
AS
SELECT party_id                                         AS party_id,
       country_cd                                       AS country_cd,
       state                                            AS state,
       business_unit                                    AS business_unit,
       merch_tax_reg                                    AS merch_tax_reg,
       valid_from                                       AS valid_from,
       valid_to                                         AS valid_to,
       cre_dttm                                         AS cre_dttm,
       last_upd_dttm                                    AS last_upd_dttm
FROM party p
WHERE EXISTS(
              SELECT 1
              FROM vw_batch_history r
              WHERE r.batch_code = p.batch_code
                AND r.attempt = p.batch_attempt
          );