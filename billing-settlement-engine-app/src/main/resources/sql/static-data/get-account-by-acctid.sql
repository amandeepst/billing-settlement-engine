SELECT
    settlement_region_id as processingGroup,
    bill_cyc_id as billCycleCode
FROM vw_acct
WHERE acct_id = :accountId

