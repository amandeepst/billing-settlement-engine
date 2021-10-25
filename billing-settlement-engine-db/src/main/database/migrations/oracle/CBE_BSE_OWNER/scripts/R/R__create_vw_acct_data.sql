CREATE OR REPLACE VIEW vw_billing_acct_data AS
    SELECT
        sa.sub_acct_id             AS sub_account_id,
        sa.sub_acct_type           AS sub_account_type,
        ac.acct_id                 AS account_id,
        ac.acct_type               AS account_type,
        trim(ac.party_id)          AS party_id,
        trim(ac.lcp)               AS lcp,
        trim(ac.currency_cd)       AS currency,
        ac.bill_cyc_id             AS billing_cycle_code,
        ac.settlement_region_id    AS processing_group,
        p.business_unit            AS business_unit,
        ac.valid_from              AS acct_valid_from,
        ac.valid_to                AS acct_valid_to,
        sa.valid_from              AS sa_valid_from,
        sa.valid_to                AS sa_valid_to,
        p.valid_from               AS party_valid_from,
        p.valid_to                 AS party_valid_to
    FROM
             vw_acct ac
        INNER JOIN vw_sub_acct  sa ON ( sa.acct_id = ac.acct_id
                                       AND sa.sub_acct_type = ac.acct_type )
        INNER JOIN vw_party     p ON ( ac.party_id = p.party_id )
    WHERE
            sa.status <> 'INACTIVE'
        AND ac.status <> 'INACTIVE';