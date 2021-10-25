SELECT /*+ :hints */
    acct_id              AS accountId,
    withhold_fund_type   AS withholdFundType,
    withhold_fund_target AS withholdFundTarget,
    withhold_fund_prcnt  AS withholdFundPercentage
FROM vw_withhold_funds
WHERE :logical_date >= valid_from
  AND :logical_date < NVL(valid_to, TO_DATE('9999-12-31', 'yyyy/mm/dd'))