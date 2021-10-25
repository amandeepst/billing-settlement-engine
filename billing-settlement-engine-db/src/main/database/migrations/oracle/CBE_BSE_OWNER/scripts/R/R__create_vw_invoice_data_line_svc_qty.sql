CREATE OR REPLACE VIEW vw_invoice_data_line_svc_qty AS
SELECT bill_id            AS bill_id,
       bill_ln_id         AS bseg_id,
       svc_qty_cd         AS sqi_cd,
       svc_qty            AS svc_qty,
       svc_qty_type_descr as sqi_descr,
       cre_dttm           AS upload_dttm,
       'Y   '             AS extract_flg,
       null               AS extract_dttm,
       ilm_dt             AS ilm_dt,
       ilm_arch_sw        AS ilm_arch_sw
FROM bill_line_svc_qty b
WHERE svc_qty_cd IN ('TXN_AMT', 'TXN_VOL', 'F_M_AMT')
  AND EXISTS(
        SELECT 1
        FROM outputs_registry r
        WHERE r.batch_code = b.batch_code
          AND r.batch_attempt = b.batch_attempt
          AND r.dataset_id = 'BILL'
          AND r.visible = 'Y'
    );