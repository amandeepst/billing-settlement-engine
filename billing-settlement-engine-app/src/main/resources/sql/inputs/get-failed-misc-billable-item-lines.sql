SELECT /*+ :hints */
    b.bill_item_id                        AS billableItemId,
    b.line_calc_type                      AS lineCalculationType,
    b.amount                              AS amount,
    b.price                               AS price,
    ora_hash(b.bill_item_id, :partitions) AS partitionId

FROM vw_bill_item_error e
INNER JOIN vw_misc_bill_item_ln b ON e.bill_item_id = b.bill_item_id
WHERE e.ilm_dt >= :low
  AND e.ilm_dt < :high
  AND e.retry_count <= :maxAttempts
  AND b.ilm_dt >=
      (SELECT TRUNC(MIN(billable_item_ilm_dt)) FROM vw_bill_item_error where ilm_dt > :low and ilm_dt <= :high)
  AND b.ilm_dt <
      (SELECT TRUNC(MAX(billable_item_ilm_dt))+1 FROM vw_bill_item_error where ilm_dt > :low and ilm_dt <= :high)