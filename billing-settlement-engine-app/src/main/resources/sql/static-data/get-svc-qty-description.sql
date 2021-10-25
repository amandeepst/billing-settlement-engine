SELECT
    TRIM(sqi_cd)    AS serviceQuantityCode,
    descr           AS description
FROM ci_sqi_l
WHERE language_cd = 'ENG'