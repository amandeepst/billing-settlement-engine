SELECT /*+ :hints */
    b.billable_item_id               AS billableItemId,
    b.sub_account_id                 AS subAccountId,
    b.legal_counterparty             AS legalCounterparty,
    b.accrued_date                   AS accruedDate,
    trim(b.billing_currency)         AS billingCurrency,
    trim(b.price_currency)           AS priceCurrency,
    trim(b.currency_from_scheme)     AS currencyFromScheme,
    trim(b.funding_currency)         AS fundingCurrency,
    trim(b.txn_currency)             AS transactionCurrency,
    b.price_asgn_id                  AS priceAssignId,
    b.settlement_level_type          AS settlementLevelType,
    b.settlement_granularity         AS settlementGranularity,
    b.sett_level_granularity         AS settlementLevelGranularity,
    b.product_class                  AS productClass,
    b.child_product                  AS childProduct,
    b.merchant_amount_signage        AS merchantAmountSignage,
    b.merchant_code                  AS merchantCode,
    b.aggregation_hash               AS aggregationHash,
    b.partition_id                   AS partitionId,
    b.priceitem_cd                   AS priceItemCode,
    CAST(b.ilm_dt AS DATE)           AS ilmDate,
    CAST(e.first_failure_on AS DATE) AS firstFailureOn,
    e.retry_count                    AS retryCount
FROM vw_bill_item_error e
INNER JOIN vw_billable_item b
ON e.bill_item_id = b.billable_item_id
WHERE e.ilm_dt >= :low
  AND e.ilm_dt < :high
  AND e.retry_count <= :maxAttempts
AND b.ilm_dt >=
    (SELECT TRUNC(MIN(billable_item_ilm_dt)) FROM vw_bill_item_error where ilm_dt > :low and ilm_dt <= :high)
  AND b.ilm_dt <
      (SELECT TRUNC(MAX(billable_item_ilm_dt))+1 FROM vw_bill_item_error where ilm_dt > :low and ilm_dt <= :high)
