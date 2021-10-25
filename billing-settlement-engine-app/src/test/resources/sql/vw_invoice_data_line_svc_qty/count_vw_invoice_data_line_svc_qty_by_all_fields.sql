SELECT COUNT(*)
FROM vw_invoice_data_line_svc_qty
WHERE bill_id = :bill_id
  AND bseg_id = :bill_ln_id
  AND sqi_cd = :svc_qty_cd
  AND svc_qty = :svc_qty
  AND sqi_descr = :svc_qty_type_descr
  AND upload_dttm IS NOT NULL
  AND ilm_dt IS NOT NULL
  AND ilm_arch_sw = 'Y'