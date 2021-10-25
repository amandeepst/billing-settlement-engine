CREATE OR REPLACE VIEW vw_billable_item_line
AS
SELECT  billable_item_id,
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
        partition as partition_id
FROM billable_charge_line;