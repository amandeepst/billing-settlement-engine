SELECT /*+ :hints */
    bill_party_id                        as billPartyId,
    legal_counterparty                   as legalCounterparty,
    currency                             as currency,
    ora_hash(bill_party_id, :partitions) as partitionId
FROM vw_min_charge_bill
WHERE logical_date = :logicalDate
