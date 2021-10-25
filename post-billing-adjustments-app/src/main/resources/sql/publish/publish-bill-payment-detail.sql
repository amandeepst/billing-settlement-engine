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
SELECT /*+ :select-hints */
    pay_dtl_id_sq.nextval,
    SYSDATE,
    trunc(SYSDATE),
    post_bill_adj_id,
    bill_id,
    '1',
    bill_amt,
    bill_amt,
    amount,
    bill_amt + amount,
    currency_cd,
    'WAF',
    decode(sign(bill_amt), 1, 'DR', 'CR'),
    cre_dttm,
    'Y',
    null,
    'PENDING',
    null,
    0,
    0,
    ' ',
    ' ',
    null
FROM post_bill_adj_accounting
WHERE batch_code = :batch_code
  AND batch_attempt = :batch_attempt
  AND ilm_dt = :ilm_dt
  AND sub_acct_type = 'FUND'