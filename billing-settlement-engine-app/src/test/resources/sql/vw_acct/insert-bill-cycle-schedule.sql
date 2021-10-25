INSERT INTO ci_bill_cyc_sch (
    bill_cyc_cd,
    win_start_dt,
    win_end_dt,
    accounting_dt,
    est_dt,
    freeze_complete_sw,
    version
) VALUES (
    :bill_cyc_cd,
    :win_start_dt,
    :win_end_dt,
    null,
    null,
    null,
    null
);