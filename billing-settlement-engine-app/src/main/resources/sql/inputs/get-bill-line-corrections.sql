SELECT /*+ :hints */
    b.bill_ln_id         AS billLineId,
    c.bill_id            AS billId,
    b.bill_line_party_id AS billLineParty,
    b.class              AS productClass,
    b.product_id         AS productId,
    b.product_descr      AS productDescription,
    b.price_ccy          AS pricingCurrency,
    b.fund_ccy           AS fundingCurrency,
    b.fund_amt           AS fundingAmount,
    b.txn_ccy            AS transactionCurrency,
    b.txn_amt            AS transactionAmount,
    b.qty                AS quantity,
    b.price_ln_id        AS priceLineId,
    b.merchant_code      AS merchantCode,
    b.total_amount       AS totalAmount,
    b.tax_stat           AS taxStatus,
    b.prev_bill_ln       AS previousBillLine,
    b.partition          AS partitionId
FROM bill_line b
         INNER JOIN cm_inv_recalc_stg c
                    ON b.bill_id = c.bill_id
WHERE c.upload_dttm >= :low
  AND c.upload_dttm < :high
  AND UPPER(c.type) = 'CANCEL'
