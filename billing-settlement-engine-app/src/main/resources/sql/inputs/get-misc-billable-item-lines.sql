SELECT /*+ :hints */
  bill_item_id                        AS billableItemId,
  line_calc_type                      AS lineCalculationType,
  amount                              AS amount,
  price                               AS price,
  ora_hash(bill_item_id, :partitions) AS partitionId
FROM vw_misc_bill_item_ln
WHERE ilm_dt >= :low
  AND ilm_dt <  :high