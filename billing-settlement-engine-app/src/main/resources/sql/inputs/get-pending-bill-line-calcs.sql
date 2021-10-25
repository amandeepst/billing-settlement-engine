SELECT /*+ :hints */
    bill_line_calc_id AS billLineCalcId,
    bill_ln_id        AS billLineId,
    bill_id           AS billId,
    calc_ln_class     AS calculationLineClassification,
    calc_ln_type      AS calculationLineType,
    amount,
    include_on_bill   AS includeOnBill,
    rate_type         AS rateType,
    rate_val          AS rateValue,
    partition_id      AS partitionId
FROM vw_pending_bill_line_calc_s