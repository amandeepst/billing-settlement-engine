SELECT
  bill_cyc_cd AS billCycleCode,
  win_start_dt     AS winStartDate,
  win_end_dt       AS winEndDate
FROM ci_bill_cyc_sch
WHERE :minDate  <= win_start_dt
AND :maxDate >= win_end_dt
