INSERT INTO vw_pending_min_charge_s (
    legal_counterparty,
    txn_party_id,
    currency
)
VALUES (
    :lcp,
    :txn_party_id,
    :currency
)
