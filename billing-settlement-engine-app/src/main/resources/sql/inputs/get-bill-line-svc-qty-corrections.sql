SELECT /*+ :hints */
    b.bill_ln_id         AS billLineId,
    c.bill_id            AS billId,
    b.svc_qty_cd         AS serviceQuantityCode,
    b.svc_qty            AS serviceQuantity,
    b.svc_qty_type_descr AS serviceQuantityDescription,
    b.partition          AS partitionId
FROM bill_line_svc_qty b
         INNER JOIN cm_inv_recalc_stg c
                    ON b.bill_id = c.bill_id
WHERE c.upload_dttm >= :low
  AND c.upload_dttm < :high
  AND UPPER(c.type) = 'CANCEL'
