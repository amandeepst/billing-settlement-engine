SELECT price_assign_id AS priceAssignId,
        lcp AS legalCounterparty,
        party_id AS partyId,
        min_chg_period AS minChargePeriod,
        rate_tp AS rateType,
        rate AS rate,
        currency_cd as currency
FROM vw_minimum_charge
WHERE nvl(start_dt, :logicalDate) <= :logicalDate
AND nvl(end_dt, :logicalDate) >= :logicalDate
AND price_status_flag <> 'INAC'