SELECT /*+ :hints */
        nvl(ah.parent_acct_party_id, m.party_id) as billPartyId,
        m.lcp AS legalCounterparty,
        m.party_id AS txnPartyId,
        null as minChargeStartDate,
        null as minChargeEndDate,
        m.rate_tp AS minChargeType,
        0 AS applicableCharges,
        null as billDate,
        m.currency_cd as currency,
        ora_hash(m.party_id, :partitions) as partitionId
FROM vw_minimum_charge m
LEFT JOIN vw_acct acct on (trim(m.party_id) = trim(acct.party_id)
                                AND trim(m.lcp) = trim(acct.lcp)
                                AND trim(m.currency_cd) = trim(acct.currency_cd)
                                AND acct.acct_type = 'CHRG'
                                AND nvl(acct.valid_from, :logicalDate) <= :logicalDate
                                AND nvl(acct.valid_to, :logicalDate) >= :logicalDate)
LEFT JOIN vw_acct_hier ah on (acct.acct_id = ah.child_acct_id
                                AND ah.status <> 'INACTIVE'
                                AND nvl(ah.valid_from, :logicalDate) <= :logicalDate
                                AND nvl(ah.valid_to, :logicalDate) >= :logicalDate)
WHERE nvl(m.start_dt, :logicalDate) <= :logicalDate
AND nvl(m.end_dt, :logicalDate) >= :logicalDate
AND m.price_status_flag <> 'INAC'
AND (m.party_id, m.lcp, m.currency_cd) NOT IN (
    SELECT
    txn_party_id, legal_counterparty, currency
    FROM vw_pending_min_charge_s
)