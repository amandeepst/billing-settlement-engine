CREATE OR REPLACE VIEW vw_non_trans_price
AS
SELECT non_event_id       AS non_event_id,
       party_id           AS per_id_nbr,
       acct_type,
       post_bill_adj_type AS priceitem_cd,
       acct_type          AS price_category,
       amount * -1        AS calc_amt,
       currency_cd,
       bill_ref           AS bill_reference,
       'Y'                AS invoiceable_flg,
       cre_dttm           AS upload_dttm,
       'Y   '             AS extract_flg,
       null               AS extract_dttm,
       ilm_dt,
       ilm_arch_sw,
       source_key         AS source_key,
       'POST_BILL_ADJ'    AS source_type,
       post_bill_adj_id   AS source_id,
       'N'                AS credit_note_flg,
       granularity        AS sett_level_granularity,
       bill_dt            AS accrued_dt
FROM vw_post_bill_adj
UNION ALL

SELECT b.non_event_id AS non_event_id,
       b.party_id     AS per_id_nbr,
       b.acct_type,
       b.product_id   AS priceitem_cd,
       b.calc_ln_type AS price_category,
       b.amount * -1  AS calc_amt,
       b.currency_cd,
       b.bill_reference,
       b.invoiceable_flg,
       b.cre_dttm     AS upload_dttm,
       'Y   '         AS extract_flg,
       null           AS extract_dttm,
       b.ilm_dt,
       b.ilm_arch_sw,
       source_key     AS source_key,
       b.source_type,
       b.source_id,
       'N'            AS credit_note_flg,
       b.granularity  AS sett_level_granularity,
       b.accrued_dt   AS accrued_dt

FROM bill_price b
WHERE EXISTS(
              SELECT 1
              FROM outputs_registry r
              WHERE r.batch_code = b.batch_code
                AND r.batch_attempt = b.batch_attempt
                AND r.dataset_id = 'BILL'
                AND r.visible = 'Y'
          );

