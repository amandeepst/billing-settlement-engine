CREATE OR REPLACE VIEW vw_invoice_data_line_bcl AS
SELECT blc.bill_id            AS bill_id,
       blc.bill_ln_id         AS bseg_id,
       blc.calc_ln_type       AS bcl_type,
       blc.calc_ln_type_descr AS bcl_descr,
       blc.amount             AS calc_amt,
       blc.tax_stat           AS tax_stat,
       blc.tax_stat_descr     AS tax_stat_descr,
       blc.tax_rate           AS tax_rate,
       blc.cre_dttm           AS upload_dttm,
       'Y   '                 AS extract_flg,
       null                   AS extract_dttm,
       blc.ilm_dt             AS ilm_dt,
       blc.ilm_arch_sw        AS ilm_arch_sw
FROM bill_line_calc blc
WHERE include_on_bill = 'Y'
  AND EXISTS(
        SELECT 1
        FROM outputs_registry r
        WHERE r.batch_code = blc.batch_code
          AND r.batch_attempt = blc.batch_attempt
          AND r.dataset_id = 'BILL'
          AND r.visible = 'Y'
    )
UNION ALL
SELECT blsq.bill_id            AS bill_id,
       blsq.bill_ln_id         AS bseg_id,
       blsq.svc_qty_cd         AS bcl_type,
       blsq.svc_qty_type_descr AS bcl_descr,
       blsq.svc_qty            AS calc_amt,
       null                    AS tax_stat,
       null                    AS tax_stat_descr,
       null                    AS tax_rate,
       blsq.cre_dttm           AS upload_dttm,
       'Y   '                  AS extract_flg,
       null                    AS extract_dttm,
       blsq.ilm_dt             AS ilm_dt,
       blsq.ilm_arch_sw        AS ilm_arch_sw
FROM bill_line_svc_qty blsq
WHERE svc_qty_cd = 'F_M_AMT'
  AND EXISTS(
        SELECT 1
        FROM outputs_registry r
        WHERE r.batch_code = blsq.batch_code
          AND r.batch_attempt = blsq.batch_attempt
          AND r.dataset_id = 'BILL'
          AND r.visible = 'Y'
    );