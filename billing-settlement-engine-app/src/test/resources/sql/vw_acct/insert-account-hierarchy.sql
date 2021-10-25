INSERT INTO vw_acct_hier (
    parent_acct_party_id,
    parent_acct_id,
    child_acct_party_id,
    child_acct_id,
    status,
    valid_from,
    valid_to,
    cre_dttm,
    last_upd_dttm
) VALUES (
    :parent_acct_party_id,
    :parent_acct_id,
    :child_acct_party_id,
    :child_acct_id,
    :status,
    :valid_from,
    :valid_to,
    null,
    null
);