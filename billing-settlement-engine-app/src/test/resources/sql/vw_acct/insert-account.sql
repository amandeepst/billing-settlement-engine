INSERT INTO vw_acct (
    acct_id,
    party_id,
    acct_type,
    lcp,
    currency_cd,
    status,
    bill_cyc_id,
    settlement_region_id,
    valid_from,
    valid_to,
    cre_dttm,
    last_upd_dttm
) VALUES (
    :acct_id,
    :party_id,
    :acct_type,
    :lcp,
    :currency_cd,
    :status,
    :bill_cyc_id,
    :settlement_region_id,
    :valid_from,
    :valid_to,
    null,
    null
);