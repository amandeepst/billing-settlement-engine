CREATE OR REPLACE VIEW vw_invoice_data_rmb AS
SELECT bill_id           AS bill_id,
       bill_nbr          AS alt_bill_id,
       party_id          AS billing_party_id,
       substr(b.lcp, -5) AS cis_division,
       acct_type,
       business_unit     AS wpbu,
       bill_dt           AS bill_dt,
       bill_cyc_id       AS bill_cyc_cd,
       start_dt          AS win_start_dt,
       end_dt            AS win_end_dt,
       currency_cd       AS currency_cd,
       bill_amt          AS calc_amt,
       merch_tax_reg     AS merch_tax_reg_nbr,
       wp_tax_reg        AS wp_tax_reg_nbr,
       tax_type          AS tax_type,
       tax_authority     AS tax_authority,
       0                 AS previous_amt,
       prev_bill_id      AS cr_note_fr_bill_id,
       cre_dttm          AS upload_dttm,
       'Y'               AS extract_flg,
       null              AS extract_dttm,
       ilm_dt            AS ilm_dt,
       ilm_arch_sw       AS ilm_arch_sw,
       null              AS tax_bseg_id,
       null              as tax_rgme,
       settlement_region_id
FROM bill b
WHERE EXISTS(
              SELECT 1
              FROM outputs_registry r
              WHERE r.batch_code = b.batch_code
                AND r.batch_attempt = b.batch_attempt
                AND r.dataset_id = 'BILL'
                AND r.visible = 'Y'
          )
UNION ALL
SELECT bill_id,
       to_char(alt_bill_id) AS alt_bill_id,
       billing_party_id,
       cis_division,
       acct_type,
       wpbu,
       bill_dt,
       bill_cyc_cd,
       win_start_dt,
       win_end_dt,
       currency_cd,
       calc_amt,
       merch_tax_reg_nbr,
       wp_tax_reg_nbr,
       tax_type,
       tax_authority,
       previous_amt,
       cr_note_fr_bill_id,
       upload_dttm,
       extract_flg,
       extract_dttm,
       ilm_dt,
       ilm_arch_sw,
       tax_bseg_id,
       tax_rgme,
       null AS settlement_region_id
FROM cisadm.cm_invoice_data_bak;