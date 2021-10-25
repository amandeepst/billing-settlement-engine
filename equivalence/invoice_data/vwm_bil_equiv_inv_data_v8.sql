CREATE MATERIALIZED VIEW VWM_BIL_EQUIV_INV_DATA AS
WITH current_data AS (
    SELECT
        b.acct_id,
        listagg(a.bill_id, ' | ') WITHIN GROUP (ORDER BY a.bill_id) AS bill_ids,
        a.billing_party_id,
        a.cis_division,
        a.acct_type,
        a.wpbu,
        a.win_start_dt,
        a.win_end_dt,
        SUM(a.calc_amt)      AS calc_amt,
        SUM(a.calc_amt - nvl(adj_amt, 0)) AS calc_amt_wo_waf,
        SUM(nvl(adj_amt, 0)) AS adj_amt,
        SUM(tax.tax_amt) AS tax_amt,
        SUM(tax.net_amt) AS net_amt,
        TRIM(a.tax_authority) AS tax_authority,
        a.tax_type,
        a.bill_dt
    FROM
        cisadm.cm_invoice_data_bak a
        LEFT JOIN (SELECT bill_id, SUM(calc_amt) AS tax_amt, SUM(base_amt) AS net_amt FROM cisadm.cm_inv_data_tax_bak GROUP BY bill_id) tax ON a.bill_id = tax.bill_id
        INNER JOIN cisadm.ci_bill b ON a.bill_id = b.bill_id
        LEFT OUTER JOIN cisadm.cm_inv_data_adj_bak c ON a.bill_id = c.bill_id
                                                    AND TRIM(c.adj_type_cd) = 'WAFBLD'
    WHERE a.ilm_dt BETWEEN to_date('2020-12-01 22:03:24' , 'YYYY-MM-DD HH24:MI:SS') AND to_date('2021-01-01 09:18:15', 'YYYY-MM-DD HH24:MI:SS')
    AND NOT EXISTS (SELECT 1 FROM cisadm.cm_invoice_data_ln_bak l WHERE l.bill_id = a.bill_id AND l.billable_chg_id LIKE 'UP%')
    GROUP BY
        b.acct_id,
        a.billing_party_id,
        a.cis_division,
        a.acct_type,
        a.wpbu,
        a.win_start_dt,
        a.win_end_dt,
        TRIM(a.tax_authority),
        a.tax_type,
        a.bill_dt
), new_data AS (
    SELECT
        b.account_id,
        decode(COUNT(1),
            1, MIN(a.bill_id),
            2, MIN(a.bill_id) || ' | ' || MAX(a.bill_id),
            MIN(a.bill_id) || ' | ' || MAX(a.bill_id) || ' | + ' || (COUNT(1)-2) || ' others') AS bill_ids,
        a.billing_party_id,
        a.cis_division,
        a.acct_type,
        a.wpbu,
        a.win_start_dt,
        a.win_end_dt,
        SUM(a.calc_amt + nvl(c.amount, 0)) AS calc_amt,
        SUM(a.calc_amt) AS calc_amt_wo_waf,
        SUM(nvl(c.amount, 0)) AS adj_amt,
        SUM(tax.tax_amt) AS tax_amt,
        SUM(tax.net_amt) AS net_amt,
        TRIM(a.tax_authority) AS tax_authority,
        a.tax_type,
        a.bill_dt
    FROM
        cbe_bse_owner.vw_invoice_data a
        LEFT JOIN (SELECT bill_id, SUM(calc_amt) AS tax_amt, SUM(base_amt) AS net_amt FROM cbe_bse_owner.vw_invoice_data_tax GROUP BY bill_id) tax ON a.bill_id = tax.bill_id
        INNER JOIN cbe_bse_owner.bill b ON a.bill_id = b.bill_id
        LEFT OUTER JOIN cbe_bse_owner.post_bill_adj c ON a.bill_id = c.bill_id
    GROUP BY
        b.account_id,
        a.billing_party_id,
        a.cis_division,
        a.acct_type,
        a.wpbu,
        a.win_start_dt,
        a.win_end_dt,
        TRIM(a.tax_authority),
        a.tax_type,
        a.bill_dt
)
SELECT
    old.acct_id,
    old.bill_dt,
    old.bill_ids,
    new.bill_ids           AS new_bill_ids,
    old.billing_party_id,
    new.billing_party_id   AS new_billing_party_id,
    old.cis_division,
    new.cis_division       AS new_cis_division,
    old.acct_type,
    old.wpbu,
    new.wpbu               AS new_wpbu,
    old.win_start_dt,
    old.win_end_dt,
    new.win_end_dt         AS new_win_end_dt,
    old.calc_amt,
    new.calc_amt           AS new_calc_amt,
    old.calc_amt_wo_waf,
    new.calc_amt_wo_waf    AS new_calc_amt_wo_waf,
    old.adj_amt,
    new.adj_amt            AS new_adj_amt,
    old.tax_amt,
    new.tax_amt            AS new_tax_amt,
    old.net_amt,
    new.net_amt            AS new_net_amt,
    old.tax_authority,
    new.tax_authority      AS new_tax_authority,
    old.tax_type,
    new.tax_type           AS new_tax_type
FROM
    current_data old
    INNER JOIN new_data new ON old.acct_id = new.account_id AND old.bill_dt = new.bill_dt
                               AND ( old.billing_party_id <> new.billing_party_id
                                     OR old.cis_division <> new.cis_division
                                     OR old.wpbu <> new.wpbu
                                     OR old.win_end_dt <> new.win_end_dt
                                     OR old.calc_amt <> new.calc_amt
                                     OR DECODE(old.acct_type, 'CHRG', old.tax_authority, 'NA') <> DECODE(old.acct_type, 'CHRG', new.tax_authority, 'NA')
                                     OR DECODE(old.acct_type, 'CHRG', old.tax_type, 'NA') <> DECODE(old.acct_type, 'CHRG', new.tax_type, 'NA') );