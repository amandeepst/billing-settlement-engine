SELECT /*+ :hints */
    billable_item_id        as billableItemId,
    distribution_id         as distributionId,
    precise_charge_amount   as preciseChargeAmount,
    characteristic_value    as characteristicValue,
    partition_id            as partitionId,
    rate_type               as rateType,
    rate
FROM vw_billable_item_line
WHERE ilm_dt >= :low
AND ilm_dt < :high