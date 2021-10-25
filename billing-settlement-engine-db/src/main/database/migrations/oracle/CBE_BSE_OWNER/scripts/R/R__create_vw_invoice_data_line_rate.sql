CREATE OR REPLACE VIEW vw_invoice_data_line_rate AS
SELECT bill_id     AS bill_id,
       bill_ln_id  AS bseg_id,
       rate_type   AS rate_tp,
       rate_val    AS rate,
       null        as price_asgn_id,
       cre_dttm    AS upload_dttm,
       'Y   '      AS extract_flg,
       null        AS extract_dttm,
       ilm_dt      AS ilm_dt,
       ilm_arch_sw AS ilm_arch_sw
FROM bill_line_calc b
WHERE rate_type IS NOT NULL AND rate_type <> 'NA'
AND include_on_bill = 'Y'
  AND EXISTS(
        SELECT 1
        FROM outputs_registry r
        WHERE r.batch_code = b.batch_code
          AND r.batch_attempt = b.batch_attempt
          AND r.dataset_id = 'BILL'
          AND r.visible = 'Y'
    );