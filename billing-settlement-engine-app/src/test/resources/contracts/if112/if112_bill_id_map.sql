SELECT cis_division,
       per_id_nbr,
       bill_id,
       bill_start_dt,
       bill_end_dt,
       bill_reference,
       alt_bill_id,
       bill_dt,
       bill_amt,
       currency_cd,
       acct_type,
       ilm_dt
FROM cm_bill_id_map@if112_oes_ormb_dblink
WHERE ilm_dt >= TO_DATE(:c_ilm_dt, 'YYYY-MM-DD HH24:MI:SS')
  AND ilm_dt <= TO_DATE(:c_start_time, 'YYYY-MM-DD HH24:MI:SS')