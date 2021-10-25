SELECT /*+ :hints */
    b.billable_item_id        as billableItemId,
    b.distribution_id         as distributionId,
    b.precise_charge_amount   as preciseChargeAmount,
    b.characteristic_value    as characteristicValue,
    b.partition_id            as partitionId,
    b.rate_type               as rateType,
    b.rate
FROM vw_bill_item_error e
INNER JOIN vw_billable_item_line b
ON e.bill_item_id = b.billable_item_id
WHERE e.ilm_dt >= :low
  AND e.ilm_dt < :high
  AND e.retry_count <= :maxAttempts
AND b.ilm_dt >=
    (SELECT TRUNC(MIN(billable_item_ilm_dt)) FROM vw_bill_item_error where ilm_dt > :low and ilm_dt <= :high)
  AND b.ilm_dt <
      (SELECT TRUNC(MAX(billable_item_ilm_dt))+1 FROM vw_bill_item_error where ilm_dt > :low and ilm_dt <= :high)