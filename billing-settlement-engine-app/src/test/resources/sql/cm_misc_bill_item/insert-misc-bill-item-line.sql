INSERT INTO vw_misc_bill_item_ln (
    bill_item_id,
    line_calc_type,
    amount,
    price,
    ilm_dt
) VALUES (
    :bill_item_id,
    :line_calc_type,
    :amount,
    :price,
    SYSTIMESTAMP
);