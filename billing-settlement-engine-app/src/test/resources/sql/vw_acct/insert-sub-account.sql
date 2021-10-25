INSERT INTO vw_sub_acct (
    sub_acct_id,
    acct_id,
    sub_acct_type,
    status,
    valid_from,
    valid_to,
    cre_dttm,
    last_upd_dttm
) VALUES (
    :sub_acct_id,
    :acct_id,
    :sub_acct_type,
    :status,
    :valid_from,
    :valid_to,
    null,
    null
);