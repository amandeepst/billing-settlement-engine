SELECT /*+ :hints */
    bill_ln_id         AS billLineId,
    bill_id            AS billId,
    bill_line_party_id AS billLinePartyId,
    product_class      AS productClass,
    product_id         AS productIdentifier,
    price_ccy          AS pricingCurrency,
    fund_ccy           AS fundingCurrency,
    fund_amt           AS fundingAmount,
    txn_ccy            AS transactionCurrency,
    txn_amt            AS transactionAmount,
    qty                AS quantity,
    price_ln_id        AS priceLineId,
    merchant_code      AS merchantCode,
    partition_id       AS partitionId
FROM vw_pending_bill_line_s