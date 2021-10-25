SELECT
    p.party_id        AS partyId,
    p.country_cd      AS countryId,
    p.merch_tax_reg   AS merchantTaxRegistration
FROM vw_party p
WHERE NVL(p.valid_from, :logicalDate) <= :logicalDate
AND NVL(p.valid_to, :logicalDate) >= :logicalDate
