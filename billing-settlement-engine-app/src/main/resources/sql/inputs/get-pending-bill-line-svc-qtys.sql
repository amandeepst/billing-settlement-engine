SELECT /*+ :hints */
    bill_id      AS billId,
    bill_ln_id   AS billLineId,
    svc_qty_cd   AS serviceQuantityTypeCode,
    svc_qty      AS serviceQuantityValue,
    partition_id AS partitionId
FROM vw_pending_bill_line_svc_qty_s