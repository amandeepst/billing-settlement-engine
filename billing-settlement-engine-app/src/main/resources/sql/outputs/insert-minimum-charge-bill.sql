INSERT /*+ :hints */ INTO minimum_charge_bill (bill_party_id,
                                              legal_counterparty,
                                              currency,
                                              logical_date,
                                              cre_dttm,
                                              batch_code,
                                              batch_attempt,
                                              partition_id,
                                              ilm_dt,
                                              ilm_arch_sw)
VALUES (:bill_party_id,
        :legal_counterparty,
        :currency,
        :logical_date,
        SYSTIMESTAMP,
        :batch_code,
        :batch_attempt,
        :partition_id,
        :ilm_dt,
        'Y')