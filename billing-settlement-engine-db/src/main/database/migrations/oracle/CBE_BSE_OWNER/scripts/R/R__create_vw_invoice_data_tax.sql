CREATE OR REPLACE VIEW vw_invoice_data_tax AS
SELECT bill_id        AS bill_id,
       tax_amt        AS calc_amt,
       net_amt        AS base_amt,
       tax_stat       AS tax_stat,
       tax_stat_descr AS tax_stat_descr,
       tax_rate       AS tax_rate,
       rev_chg_flg    AS reverse_chrg_sw,
       null           AS char_val,
       null           AS tax_stat_char,
       cre_dttm       AS upload_dttm,
       'Y   '         AS extract_flg,
       null           AS extract_dttm,
       ilm_dt         AS ilm_dt,
       ilm_arch_sw    AS ilm_arch_sw
FROM bill_tax_detail b
WHERE EXISTS(
              SELECT 1
              FROM outputs_registry r
              WHERE r.batch_code = b.batch_code
                AND r.batch_attempt = b.batch_attempt
                AND r.dataset_id = 'BILL'
                AND r.visible = 'Y'
          );