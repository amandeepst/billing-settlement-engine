INSERT INTO vw_billable_item_svc_qty (
    billable_item_id,
    sqi_cd,
    rate_schedule,
    svc_qty,
    ilm_dt,
    partition_id
) VALUES (
    :billable_item_id,
    :sqi_cd,
    :rate_schedule,
    :svc_qty,
    SYSTIMESTAMP,
    :partition_id
);