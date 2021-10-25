CREATE OR REPLACE VIEW vw_pending_bill_line_calc
AS
SELECT bill_line_calc_id,
       bill_ln_id,
       bill_id,
       calc_ln_class,
       calc_ln_type,
       amount,
       include_on_bill,
       rate_type,
       rate_val,
       batch_code,
       batch_attempt,
       ilm_dt,
       ilm_arch_sw,
       partition AS partition_id
FROM pending_bill_line_calc b
WHERE EXISTS(
              SELECT 1
              FROM outputs_registry r
              WHERE r.batch_code = b.batch_code
                AND r.batch_attempt = b.batch_attempt
                AND r.dataset_id = 'PENDING_BILL'
                AND r.visible = 'Y'
          );