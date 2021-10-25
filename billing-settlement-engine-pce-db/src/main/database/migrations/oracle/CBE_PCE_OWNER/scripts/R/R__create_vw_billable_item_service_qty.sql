CREATE OR REPLACE VIEW vw_billable_item_svc_qty
AS
SELECT  billable_item_id,
        sqi_cd,
        rate_schedule,
        svc_qty,
        ilm_dt,
        partition as partition_id
FROM billable_charge_service_output;