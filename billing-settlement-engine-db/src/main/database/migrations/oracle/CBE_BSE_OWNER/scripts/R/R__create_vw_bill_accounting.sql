CREATE OR REPLACE VIEW vw_bill_accounting
AS
  SELECT
    party_id,
    acct_id,
    acct_type,
    bill_sub_acct_id,
    sub_acct_type,
    currency_cd,
    bill_amt,
    bill_id,
    bill_ln_id,
    total_line_amount,
    lcp,
    business_unit,
    class,
    calc_ln_id,
    type,
    calc_ln_type,
    calc_line_amount,
    bill_dt,
    cre_dttm,
    ilm_dt,
    partition as partition_id
  FROM bill_accounting b
  WHERE EXISTS (
    SELECT 1
    FROM outputs_registry r
    WHERE r.batch_code    = b.batch_code
      AND r.batch_attempt = b.batch_attempt
      AND r.dataset_id    = 'BILL_ACCOUNTING'
      AND r.visible       = 'Y'
  );