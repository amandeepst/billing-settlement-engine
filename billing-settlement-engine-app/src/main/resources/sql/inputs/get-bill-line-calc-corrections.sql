SELECT /*+ :hints */
    b.bill_line_calc_id  AS billLineCalcId,
    b.bill_ln_id         AS billLineId,
    c.bill_id            AS billId,
    b.calc_ln_class      AS calculationLineClassification,
    b.calc_ln_type       AS calculationLineType,
    b.calc_ln_type_descr AS calculationLineTypeDescription,
    b.amount,
    b.include_on_bill    AS includeOnBill,
    b.rate_type          AS rateType,
    b.rate_val           AS rateValue,
    b.tax_stat           AS taxStatus,
    b.tax_rate           AS taxRate,
    b.tax_stat_descr     AS taxStatusDescription,
    b.partition          AS partitionId
FROM bill_line_calc b
         INNER JOIN cm_inv_recalc_stg c
                    ON b.bill_id = c.bill_id
WHERE c.upload_dttm >= :low
  AND c.upload_dttm < :high
  AND UPPER(c.type) = 'CANCEL'
