INSERT /*+ :hints */ INTO bill_item_error (bill_item_id,
                                           first_failure_on,
                                           fix_dt,
                                           retry_count,
                                           code,
                                           status,
                                           reason,
                                           stack_trace,
                                           cre_dttm,
                                           batch_code,
                                           batch_attempt,
                                           partition_id,
                                           billable_item_ilm_dt,
                                           ilm_dt,
                                           ilm_arch_sw)
VALUES (:bill_item_id,
        :first_failure_on,
        SYSTIMESTAMP,
        :retry_count,
        ' ',
        ' ',
        ' ',
        ' ',
        SYSTIMESTAMP,
        :batch_code,
        :batch_attempt,
        :partition_id,
        :billable_item_ilm_dt,
        :ilm_dt,
        'Y')