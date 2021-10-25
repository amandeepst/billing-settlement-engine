SELECT b.post_bill_adj_id   AS financialTransactionId,
       b.type               AS parentId,
       b.party_id           AS partyId,
       b.acct_id            AS accountId,
       b.acct_type          AS accountType,
       b.sub_acct_id        AS subAccountId,
       b.sub_acct_type      AS subAccountType,
       b.bill_id            AS billId,
       b.post_bill_adj_id   AS billSegmentId,
       'AD'                 AS financialTransactionType,
       b.amount             AS amount,
       b.lcp                AS legalCounterparty,
       b.currency_cd        AS currency,
       b.bill_dt            AS accountingDate,
       b.business_unit      AS businessUnit,
       0                    AS retryCount,
       b.partition_id       AS partitionId,
       NULL                 AS firstFailureOn
FROM vw_post_bill_adj_accounting b
WHERE ilm_dt <= :high
  AND ilm_dt >  :low