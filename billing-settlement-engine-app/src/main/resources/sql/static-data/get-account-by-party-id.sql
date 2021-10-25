SELECT
    null          AS childPartyId,
    parent_ac.sub_account_id                                 AS parentSubAccountId,
    parent_ac.sub_account_type                               AS subAccountType,
    parent_ac.account_id                                     AS accountId,
    parent_ac.account_type                                   AS accountType,
    parent_ac.party_id                                       AS partyId,
    parent_ac.lcp                                            AS legalCounterparty,
    parent_ac.currency                                       AS currencyCode,
    parent_ac.billing_cycle_code                             AS billingcycleCode,
    parent_ac.processing_group                               AS processingGroup,
    parent_ac.business_unit                                  AS businessUnit
FROM
    vwm_billing_acct_data parent_ac
WHERE parent_ac.party_id      = :partyId
    AND parent_ac.lcp          = :lcp
    AND parent_ac.currency     = :currency
    AND parent_ac.account_type = 'CHRG'
    AND TRUNC(nvl(parent_ac.acct_valid_from, :logicalDate)) <= TRUNC(:logicalDate)
    AND TRUNC(nvl(parent_ac.acct_valid_to, :logicalDate)) >= TRUNC(:logicalDate)
    AND TRUNC(nvl(parent_ac.sa_valid_from, :logicalDate)) <= TRUNC(:logicalDate)
    AND TRUNC(nvl(parent_ac.sa_valid_to, :logicalDate)) >= TRUNC(:logicalDate)
    AND TRUNC(nvl(parent_ac.party_valid_from, :logicalDate)) <= TRUNC(:logicalDate)
    AND TRUNC(nvl(parent_ac.party_valid_to, :logicalDate)) >= TRUNC(:logicalDate)