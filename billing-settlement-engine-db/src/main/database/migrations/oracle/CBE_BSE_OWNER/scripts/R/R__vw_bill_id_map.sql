CREATE OR REPLACE VIEW vw_bill_id_map
AS
SELECT '42'                                      AS event_type_id,
       SUBSTR(b.lcp, -LEAST(LENGTH(b.lcp), 5))   AS cis_division,
       b.party_id                                AS per_id_nbr,
       'ORMB'                                    AS event_process_id,
       b.bill_ref                                AS bill_reference,
       b.bill_id                                 AS bill_id,
       b.start_dt                                AS bill_start_dt,
       b.end_dt                                  AS bill_end_dt,
       b.bill_nbr                                AS alt_bill_id,
       NVL(b.debt_dt, b.bill_dt)                 AS bill_dt,
       b.bill_amt                                AS bill_amt,
       b.currency_cd                             AS currency_cd,
       b.prev_bill_id                            AS cr_note_fr_bill_id,
       b.cre_dttm                                AS upload_dttm,
       'Y   '                                    AS extract_flg,
       NULL                                      AS extract_dttm,
       b.acct_type                               AS acct_type,
       b.ilm_dt                                  AS ilm_dt,
       b.ilm_arch_sw                             AS ilm_arch_sw,
       b.bill_map_id                             AS bill_map_id,
       b.settlement_region_id                    AS settlement_region_id
FROM
  vw_bill b;