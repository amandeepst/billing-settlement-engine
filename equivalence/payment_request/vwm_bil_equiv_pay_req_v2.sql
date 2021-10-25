CREATE MATERIALIZED VIEW VWM_BIL_EQUIV_PAY_REQ AS
WITH old_data AS (
      SELECT
          a.bill_dt,
          a.bill_id,
          a.currency_cd,
          a.per_id_nbr,
          a.cis_division,
          a.acct_type,
          a.bill_amt,
          a.is_ind_flg,
          a.is_imd_fin_adj,
          a.fin_adj_man_nrt,
          a.rel_rsrv_flg,
          a.rel_waf_flg,
          a.case_identifier,
          a.sub_stlmnt_lvl_ref,
          b.granularity_key,
          nvl2(c.adj_amt, 'WAF', 'NO_WAF') AS WAF_IND
      FROM
          cisadm.cm_pay_req_bak a
          LEFT JOIN (
              SELECT
                  bill_id,
                  line_id,
                  LISTAGG(granularities_type
                          || '|'
                          || reference_val, ',') WITHIN GROUP(
                      ORDER BY
                          granularities_type
                  ) AS granularity_key
              FROM
                  cisadm.cm_pay_req_granularities_bak
              GROUP BY
                  bill_id,
                  line_id
          ) b ON a.bill_id = b.bill_id
                 AND a.line_id = b.line_id
                 left outer join cisadm.cm_inv_data_adj_bak c
                 on a.bill_id = c.bill_id
      WHERE a.ilm_dt BETWEEN to_date('2020-12-01 22:02:42' , 'YYYY-MM-DD HH24:MI:SS') AND to_date('2021-01-01 08:47:05', 'YYYY-MM-DD HH24:MI:SS')
        AND bill_dt between '01-DEC-20' and '31-DEC-20'
  ), new_data AS (
      SELECT
          a.bill_dt,
          a.bill_id,
          a.per_id_nbr,
          a.currency_cd,
          a.cis_division,
          a.acct_type,
          a.bill_amt,
          a.is_ind_flg,
          a.is_imd_fin_adj,
          a.fin_adj_man_nrt,
          a.rel_rsrv_flg,
          a.rel_waf_flg,
          a.case_identifier,
          a.sub_stlmnt_lvl_ref,
          b.granularity_key,
          nvl2(c.post_bill_adj_id, 'WAF', 'NO_WAF') AS WAF_IND
      FROM
          cbe_bse_owner.vw_payment_request a
          LEFT JOIN (
              SELECT
                  bill_id,
                  line_id,
                  LISTAGG(granularities_type
                          || '|'
                          || reference_val, ',') WITHIN GROUP(
                      ORDER BY
                          granularities_type
                  ) AS granularity_key
              FROM
                  CBE_BSE_OWNER.vw_payment_request_granularity
              GROUP BY
                  bill_id,
                  line_id
          ) b ON a.bill_id = b.bill_id
                 AND a.line_id = b.line_id
                   left outer join CBE_BSE_OWNER.post_bill_adj c
                 on a.bill_id = c.bill_id
      WHERE 1=1
         -- ilm_dt > SYSDATE - 10
  )
  SELECT /*+parallel(16)*/
      new.waf_ind as new_waf_ind,
      old.waf_ind as old_waf_ind,
      decode(new.waf_ind, old.waf_ind, 'NO_WAF_DIFF', 'WAF_ENV_DIFF') AS WAF_ENV_CHECK,
      old.bill_dt as old_bill_dt,
      new.bill_dt as new_bill_dt,
      new.bill_id as new_bill_id,
      old.bill_id as old_bill_id,
      new.per_id_nbr as new_party_id,
      old.per_id_nbr as old_party_id,
      new.cis_division as new_division,
      old.cis_division as old_division,
      new.acct_type as new_acct_type,
      old.acct_type as old_acct_type,
      new.bill_amt as new_bill_amt,
      old.bill_amt as old_bill_amt,
      new.is_ind_flg as new_ind_flg,
      old.is_ind_flg as old_ind_flg,
      new.is_imd_fin_adj as new_imd_fin_adj_flg,
      old.is_imd_fin_adj as old_imd_fin_adj_flg,
      new.fin_adj_man_nrt as new_fin_adj_man_nrt,
      old.fin_adj_man_nrt as old_fin_adj_man_nrt,
      new.rel_rsrv_flg as new_rel_rsrv_flg,
      old.rel_rsrv_flg as old_rel_rsrv_flg,
      new.rel_waf_flg as new_rel_waf_flg,
      old.rel_waf_flg as old_rel_waf_flg,
      new.case_identifier as new_case_identifier,
      old.case_identifier as old_case_identifier,
      new.granularity_key as new_granularity_key,
      old.granularity_key as old_granularity_key,
      new.sub_stlmnt_lvl_ref as new_sett_sub_ref,
      old.sub_stlmnt_lvl_ref as old_sett_sub_ref
  FROM
      old_data old
      left OUTER JOIN new_data new ON nvl(old.granularity_key, 'x') = nvl(new.granularity_key, 'x')
      and old.bill_dt = new.bill_dt
      and old.per_id_nbr = new.per_id_nbr
      and nvl(old.sub_stlmnt_lvl_ref,'X') =  nvl(new.sub_stlmnt_lvl_ref,'X')
      and old.cis_division = new.cis_division
      and old.ACCT_TYPE = new.ACCT_TYPE
      and old.currency_cd = new.currency_cd
  WHERE
      ( old.per_id_nbr <> new.per_id_nbr
        OR old.cis_division <> new.cis_division
        OR old.acct_type <> new.acct_type
        OR old.bill_amt <> new.bill_amt
        OR old.is_ind_flg <> new.is_ind_flg
        OR old.is_imd_fin_adj <> new.is_imd_fin_adj
        OR old.fin_adj_man_nrt <> new.fin_adj_man_nrt
        OR old.rel_rsrv_flg <> new.rel_rsrv_flg
        OR old.rel_waf_flg <> new.rel_waf_flg
        OR old.case_identifier <> new.case_identifier
        or new.per_id_nbr is null
        or old.per_id_nbr is null)
        order by old.per_id_nbr, old.bill_dt;