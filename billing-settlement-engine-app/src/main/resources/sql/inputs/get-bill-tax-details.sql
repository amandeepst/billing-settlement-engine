SELECT /*+ :hints */
    b.bill_tax_id    AS billTaxId,
    b.tax_stat       AS taxStatus,
    b.tax_rate       AS taxRate,
    b.tax_stat_descr AS taxStatusDescription,
    b.net_amt        AS netAmount,
    b.tax_amt        AS taxAmount,
    b.partition      AS partitionId
FROM bill_tax_detail b
         INNER JOIN cm_inv_recalc_stg c ON b.bill_id = c.bill_id
WHERE c.upload_dttm >= :low
  AND c.upload_dttm < :high
  AND UPPER(c.type) = 'CANCEL'