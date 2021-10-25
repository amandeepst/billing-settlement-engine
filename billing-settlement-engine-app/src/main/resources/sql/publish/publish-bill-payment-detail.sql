INSERT INTO cm_bill_payment_dtl (
    pay_dtl_id,
    upload_dttm,
    pay_dt,
    ext_transmit_id,
    bill_id,
    line_id,
    line_amt,
    prev_unpaid_amt,
    pay_amt,
    unpaid_amt,
    currency_cd,
    status_cd,
    pay_type,
    ilm_dt,
    ilm_arch_sw,
    overpaid,
    record_stat,
    status_upd_dttm,
    message_cat_nbr,
    message_nbr,
    error_info,
    ext_source_cd,
    credit_note_id
)
WITH latest_bill_payment_dtl AS (
    SELECT * FROM cm_bill_payment_dtl WHERE EXISTS
        (SELECT 1 FROM
            (SELECT MAX(pay_dtl_id) AS latest_pay_dtl_id FROM cm_bill_payment_dtl GROUP BY bill_id)
            WHERE latest_pay_dtl_id = pay_dtl_id)
)
SELECT /*+ :select-hints */
    pay_dtl_id_sq.nextval,                                                                  --pay_dtl_id
    SYSDATE,                                                                                --upload_dttm
    trunc(SYSDATE),                                                                         --pay_dt
    null,                                                                                   --ext_transmit_id
    nvl2(b.prev_bill_id, b.prev_bill_id, b.bill_id),                                        --bill_id
    '1',                                                                                    --line_id
    nvl(bpd.line_amt, b.bill_amt),                                                          --line_amt
    nvl(bpd.unpaid_amt, b.bill_amt),                                                        --prev_unpaid_amt
    nvl2(b.prev_bill_id, b.bill_amt, 0),                                                    --pay_amt
    nvl(bpd.unpaid_amt, 0) + b.bill_amt,                                                    --unpaid_amt
    b.currency_cd,                                                                          --currency_cd
    nvl2(b.prev_bill_id, 'CANCELLED',
        nvl2(b.debt_dt, 'MIGRATED_DEBT', 'REQUEST')),                                       --status_cd
    decode(sign(nvl(bpd.line_amt, b.bill_amt)), 1, 'DR', 'CR'),                             --pay_type
    b.cre_dttm,                                                                             --ilm_dt
    'Y',                                                                                    --ilm_arch_sw
    CASE
      WHEN
        (nvl(bpd.line_amt, b.bill_amt) >= 0 AND (nvl(bpd.unpaid_amt, 0) + b.bill_amt) < 0)
        OR
        (nvl(bpd.line_amt, bill_amt) < 0 AND (nvl(bpd.unpaid_amt, 0) + bill_amt) >= 0)
      THEN 'Y'
      ELSE NULL
    END,                                                                                    --overpaid
    'PENDING',                                                                              --record_stat
    null,                                                                                   --status_upd_dttm
    '0',                                                                                    --message_cat_nbr
    '0',                                                                                    --message_nbr
    ' ',                                                                                    --error_info
    ' ',                                                                                    --ext_source_cd
    nvl2(b.prev_bill_id, b.bill_id, null)                                                   --credit_note_id
FROM bill b LEFT OUTER JOIN latest_bill_payment_dtl bpd ON b.prev_bill_id = bpd.bill_id
WHERE b.batch_code = :batch_code
  AND b.batch_attempt = :batch_attempt
  AND b.ilm_dt = :ilm_dt