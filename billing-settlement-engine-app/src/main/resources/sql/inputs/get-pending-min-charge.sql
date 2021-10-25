SELECT /*+ :hints */
    bill_party_id                        as billPartyId,
    legal_counterparty                   as legalCounterparty,
    txn_party_id                         as txnPartyId,
    min_chg_start_dt                     as minChargeStartDate,
    min_chg_end_dt                       as minChargeEndDate,
    min_chg_type                         as minChargeType,
    applicable_charges                   as applicableCharges,
    currency                             as currency,
    bill_dt                              as billDate,
    ora_hash(bill_party_id, :partitions) as partitionId
FROM vw_pending_min_charge_s
