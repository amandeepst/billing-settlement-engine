SELECT
    TRIM(priceitem_cd)  AS productCode,
    TRIM(char_type_cd)  AS characteristicType,
    TRIM(char_val)      AS characteristicValue
FROM ci_priceitem_char
WHERE TRIM(char_type_cd) LIKE 'TX%'