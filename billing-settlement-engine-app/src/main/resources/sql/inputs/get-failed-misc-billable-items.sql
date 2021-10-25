SELECT /*+ :hints */
    b.misc_bill_item_id                        AS billableItemId,
    b.sub_account_id                           AS subAccountId,
    b.lcp                                      AS legalCounterparty,
    b.accrued_dt                               AS accruedDate,
    b.adhoc_bill_flg                           AS adhocBillFlag,
    trim(b.currency_cd)                        AS currencyCode,
    trim(b.product_class)                      AS productClass,
    b.product_id                               AS productId,
    b.qty                                      AS quantity,
    ora_hash(b.misc_bill_item_id, :partitions) AS partitionId,
    b.rel_waf_flg                              AS releaseWAFIndicator,
    b.rel_reserve_flg                          AS releaseReserveIndicator,
    b.fastest_payment_flg                      AS fastestSettlementIndicator,
    DECODE(b.ind_payment_flg, 'Y', b.case_id, 'N') AS caseIdentifier,
    b.ind_payment_flg                          AS individualPaymentIndicator,
    DECODE(b.ind_payment_flg, 'Y', b.pay_narrative, 'N') AS paymentNarrative,
    CASE
        WHEN TRIM(product_id) IN (
            'MIGCHRG',
            'MIGFUND',
            'MIGCHBK'
        ) THEN b.debt_dt
        ELSE NULL
    END                                        AS debtDate,
    b.source_type                              AS sourceType,
    b.source_id                                AS sourceId,
    CAST(b.ilm_dt AS DATE)                     AS ilmDate,
    CAST(e.first_failure_on AS DATE)           AS firstFailureOn,
    e.retry_count                              AS retryCount
FROM vw_bill_item_error e
INNER JOIN vw_misc_bill_item b ON e.bill_item_id = b.misc_bill_item_id
WHERE e.ilm_dt >= :low
  AND e.ilm_dt < :high
  AND e.retry_count <= :maxAttempts
  AND b.ilm_dt >=
      (SELECT TRUNC(MIN(billable_item_ilm_dt)) FROM vw_bill_item_error where ilm_dt > :low and ilm_dt <= :high)
  AND b.ilm_dt <
      (SELECT TRUNC(MAX(billable_item_ilm_dt))+1 FROM vw_bill_item_error where ilm_dt > :low and ilm_dt <= :high)