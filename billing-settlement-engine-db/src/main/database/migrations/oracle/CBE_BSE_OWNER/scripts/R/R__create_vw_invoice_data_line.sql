CREATE OR REPLACE VIEW vw_invoice_data_line AS
SELECT bill_id            AS bill_id,
       bill_ln_id         AS bseg_id,
       bill_line_party_id AS billing_party_id,
       product_id         AS price_category,
       price_ccy          AS currency_cd,
       product_descr      AS price_category_descr,
       merchant_code      AS merchant_code,
       cre_dttm           AS upload_dttm,
       'Y   '             AS extract_flg,
       null               AS extract_dttm,
       ilm_dt             AS ilm_dt,
       ilm_arch_sw        AS ilm_arch_sw,
       nvl(fund_ccy,price_ccy)           AS udf_char_25,
       0                  AS billable_chg_id,
       0                  AS sett_level_granularity
FROM bill_line b
WHERE EXISTS(
              SELECT 1
              FROM outputs_registry r
              WHERE r.batch_code = b.batch_code
                AND r.batch_attempt = b.batch_attempt
                AND r.dataset_id = 'BILL'
                AND r.visible = 'Y'
          );