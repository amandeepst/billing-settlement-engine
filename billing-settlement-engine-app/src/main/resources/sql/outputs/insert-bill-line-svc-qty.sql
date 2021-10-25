INSERT INTO bill_line_svc_qty (
    bill_id,
    bill_ln_id,
    svc_qty_cd,
    svc_qty,
    svc_qty_type_descr,
    cre_dttm,
    batch_code,
    batch_attempt,
    partition_id,
    ilm_dt,
    ilm_arch_sw
) VALUES (
          :bill_id,
          :bill_ln_id,
          :svc_qty_cd,
          :svc_qty,
          :svc_qty_type_descr,
           SYSDATE,
          :batch_code,
          :batch_attempt,
          :partition_id,
          :ilm_dt,
           'Y'
         )