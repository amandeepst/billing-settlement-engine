INSERT INTO vw_minimum_charge (
    price_assign_id,
    lcp,
    party_id,
    min_chg_period,
    rate_tp,
    rate,
    start_dt,
    end_dt,
    price_status_flag,
    currency_cd
) VALUES (
    :price_assign_id,
    :lcp,
    :party_id,
    :min_chg_period,
    :rate_tp,
    :rate,
    :start_dt,
    :end_dt,
    :price_status_flag,
    :currency
)
