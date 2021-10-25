INSERT INTO vw_party (
    party_id,
    country_cd,
    state,
    business_unit,
    merch_tax_reg,
    valid_from,
    valid_to,
    cre_dttm,
    last_upd_dttm
) VALUES (
    :party_id,
    :country_cd,
    null,
    :business_unit,
    :merch_tax_reg,
    :valid_from,
    :valid_to,
    null,
    null
);