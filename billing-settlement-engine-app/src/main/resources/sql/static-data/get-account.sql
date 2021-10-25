SELECT
    nvl(ah.child_acct_party_id, parent_ac.party_id)          AS childPartyId,
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
    vw_sub_acct        child_sa
    LEFT OUTER JOIN vw_acct_hier       ah ON ( child_sa.acct_id = ah.child_acct_id
                                         AND ah.status <> 'INACTIVE'
                                         AND nvl(ah.valid_from, :logicalDate) <= :logicalDate
                                         AND nvl(ah.valid_to, :logicalDate) >= :logicalDate )
    INNER JOIN vwm_billing_acct_data parent_ac ON ( nvl(ah.parent_acct_id, child_sa.acct_id) = parent_ac.account_id )
WHERE
        child_sa.status <> 'INACTIVE'
    AND child_sa.sub_acct_id = :subAcctId
    AND TRUNC(nvl(child_sa.valid_from, :logicalDate)) <= TRUNC(:logicalDate)
    AND TRUNC(nvl(child_sa.valid_to, :logicalDate)) >= TRUNC(:logicalDate)
    AND TRUNC(nvl(parent_ac.acct_valid_from, :logicalDate)) <= TRUNC(:logicalDate)
    AND TRUNC(nvl(parent_ac.acct_valid_to, :logicalDate)) >= TRUNC(:logicalDate)
    AND TRUNC(nvl(parent_ac.sa_valid_from, :logicalDate)) <= TRUNC(:logicalDate)
    AND TRUNC(nvl(parent_ac.sa_valid_to, :logicalDate)) >= TRUNC(:logicalDate)
    AND TRUNC(nvl(parent_ac.party_valid_from, :logicalDate)) <= TRUNC(:logicalDate)
    AND TRUNC(nvl(parent_ac.party_valid_to, :logicalDate)) >= TRUNC(:logicalDate)