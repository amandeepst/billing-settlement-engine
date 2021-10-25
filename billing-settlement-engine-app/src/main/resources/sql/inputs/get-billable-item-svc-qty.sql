SELECT /*+ :hints */
    billable_item_id    AS billableItemId,
    sqi_cd              AS sqiCd,
    rate_schedule       AS rateSchedule,
    svc_qty             AS serviceQuantity,
    partition_id        AS partitionId
FROM vw_billable_item_svc_qty
WHERE ilm_dt >= :low
AND ilm_dt < :high