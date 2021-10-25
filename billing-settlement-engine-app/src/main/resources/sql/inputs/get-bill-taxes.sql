SELECT /*+ :hints */
    b.bill_tax_id   AS billTaxId,
    b.bill_id       AS billId,
    b.merch_tax_reg AS merchantTaxRegistrationNumber,
    b.wp_tax_reg    AS worldpayTaxRegistrationNumber,
    b.tax_type      AS taxType,
    b.tax_authority AS taxAuthority,
    b.rev_chg_flg   AS reverseChargeFlag,
    b.partition     AS partitionId
FROM bill_tax b
         INNER JOIN cm_inv_recalc_stg c ON b.bill_id = c.bill_id
WHERE c.upload_dttm >= :low
  AND c.upload_dttm < :high
  AND UPPER(c.type) = 'CANCEL'