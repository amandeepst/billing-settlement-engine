DELETE FROM charge_type_classifier
WHERE
    chg_type_classifier_id = :char_val
    AND chg_type_class_grp = :char_type_cd
    AND chg_type = :priceitem_cd