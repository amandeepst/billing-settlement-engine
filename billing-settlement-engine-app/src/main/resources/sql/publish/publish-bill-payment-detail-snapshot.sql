MERGE /*+ :insert-hints */ INTO cm_bill_payment_dtl_snapshot s
USING (
WITH latest_bill_payment_dtl AS (
    SELECT * FROM cm_bill_payment_dtl WHERE EXISTS
        (SELECT 1 FROM
            (SELECT MAX(pay_dtl_id) AS latest_pay_dtl_id FROM cm_bill_payment_dtl GROUP BY bill_id)
            WHERE latest_pay_dtl_id = pay_dtl_id)
)
SELECT b.*,
       bpd.pay_dtl_id,
       bpd.status_cd,
       bpd.prev_unpaid_amt,
       bpd.pay_amt,
       bpd.unpaid_amt,
       bpd.line_id,
       bpd.line_amt,
       bpd.pay_type,
       bpd.overpaid,
       bpd.record_stat,
       bpd.pay_dt
    FROM bill b INNER JOIN latest_bill_payment_dtl bpd
                        ON nvl2(b.prev_bill_id, b.prev_bill_id, b.bill_id) = bpd.bill_id
    WHERE b.batch_code    = :batch_code
      AND b.batch_attempt = :batch_attempt
      AND b.ilm_dt        = :ilm_dt
) b
ON (nvl2(b.prev_bill_id, b.prev_bill_id, b.bill_id) = s.bill_id
    AND s.line_id = '1')
WHEN MATCHED THEN
    UPDATE SET  s.prev_unpaid_amt   = b.prev_unpaid_amt,
                s.latest_pay_amt    = b.pay_amt,
                s.unpaid_amt        = b.unpaid_amt,
                s.bill_balance      = b.unpaid_amt,
                s.latest_status     = b.status_cd,
                s.latest_pay_dtl_id = b.pay_dtl_id,
                s.overpaid          = b.overpaid,
                s.pay_dt = b.pay_dt
WHEN NOT MATCHED THEN
    INSERT (bill_balance_id,
            latest_pay_dtl_id,
            latest_upload_dttm,
            pay_dt,
            bill_dt,
            party_id,
            lcp_description,
            lcp,
            acct_type,
            account_description,
            ext_transmit_id,
            bill_id,
            alt_bill_id,
            line_id,
            line_amt,
            prev_unpaid_amt,
            latest_pay_amt,
            unpaid_amt,
            bill_amount,
            bill_balance,
            currency_cd,
            latest_status,
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
    VALUES (bill_balance_id_seq.nextval,                                             --bill_balance_id
            b.pay_dtl_id,                                                            --latest_pay_dtl_id
            SYSDATE,                                                                 --latest_upload_dttm
            trunc(SYSDATE),                                                          --pay_dt
            b.bill_dt,                                                               --bill_dt
            b.party_id,                                                              --party_id
            b.lcp,                                                                   --lcp_description
            b.lcp,                                                                   --lcp
            b.acct_type,                                                             --acct_type
            decode(b.acct_type,
                'CHRG', 'Charging',
                'FUND', 'Funding',
                'CHBK', 'Chargeback',
                ' '),                                                                --account_description
            null,                                                                    --ext_transmit_id
            nvl2(b.prev_bill_id, b.prev_bill_id, b.bill_id),                         --bill_id
            b.bill_nbr,                                                              --alt_bill_id
            b.line_id,                                                               --line_id
            b.line_amt,                                                              --line_amt
            b.prev_unpaid_amt,                                                       --prev_unpaid_amt
            b.pay_amt,                                                               --latest_pay_amt
            b.unpaid_amt,                                                            --unpaid_amt
            b.bill_amt,                                                              --bill_amount
            b.unpaid_amt,                                                            --bill_balance
            b.currency_cd,                                                           --currency_cd
            b.status_cd,                                                             --latest_status
            b.pay_type,                                                              --pay_type
            b.cre_dttm,                                                              --ilm_dt
            'Y',                                                                     --ilm_arch_sw
            b.overpaid,                                                              --overpaid
            b.record_stat,                                                           --record_stat
            null,                                                                    --status_upd_dttm
            '0',                                                                     --message_cat_nbr
            '0',                                                                     --message_nbr
            ' ',                                                                     --error_info
            ' ',                                                                     --ext_source_cd
            nvl2(b.prev_bill_id, b.bill_id, null)                                    --credit_note_id
    )
