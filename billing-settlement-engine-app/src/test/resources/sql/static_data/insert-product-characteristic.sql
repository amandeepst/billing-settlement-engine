INSERT INTO charge_type_classifier (
    chg_type_classifier_id,
    chg_type_class_grp,
    chg_type,
    description,
    valid_from
) VALUES (
    :char_val,
    :char_type_cd,
    :priceitem_cd,
    '     ',
    sysdate
)
