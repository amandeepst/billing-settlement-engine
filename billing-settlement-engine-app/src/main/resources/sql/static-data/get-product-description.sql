SELECT
    TRIM(priceitem_cd)  AS productCode,
    descr               AS description
FROM ci_priceitem_l
WHERE language_cd = 'ENG'