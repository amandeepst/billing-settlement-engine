SELECT
    sa.acct_id      AS accountId,
    sa.sub_acct_id  AS subAccountId
FROM
    vw_sub_acct sa,
    vw_withhold_funds w
WHERE
    sa.acct_id = w.acct_id
    AND :logicalDate >= w.valid_from
    AND :logicalDate < NVL(w.valid_to, TO_DATE('9999-12-31', 'yyyy/mm/dd'))
    AND trim(sa.sub_acct_type) = 'WAF'
    AND sa.status <> 'INACTIVE'
    AND nvl(sa.valid_from, :logicalDate) <= :logicalDate
    AND nvl(sa.valid_to, :logicalDate) >= :logicalDate