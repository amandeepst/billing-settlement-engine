INSERT INTO vw_billable_item_line (
    billable_item_id,
    line_sequence,
    charge_amount,
    currency_cd,
    distribution_id,
    precise_charge_amount,
    ilm_dt,
    description_on_bill,
    characteristic_value,
    rate_type,
    rate,
    partition_id
) VALUES (
    :billable_item_id,
    null,
    null,
    null,
    :distribution_id,
    :precise_charge_amount,
    SYSTIMESTAMP,
    null,
    :characteristic_value,
    :rate_type,
    :rate,
    :partition_id
);