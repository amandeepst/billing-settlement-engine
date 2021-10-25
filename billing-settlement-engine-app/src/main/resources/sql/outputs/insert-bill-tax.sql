INSERT /*+ :hints */ INTO bill_tax (bill_tax_id,
                                    bill_id,
                                    merch_tax_reg,
                                    wp_tax_reg,
                                    tax_type,
                                    tax_authority,
                                    rev_chg_flg,
                                    cre_dttm,
                                    partition_id,
                                    ilm_dt,
                                    ilm_arch_sw,
                                    batch_code,
                                    batch_attempt)
VALUES (:bill_tax_id,
        :bill_id,
        :merch_tax_reg,
        :wp_tax_reg,
        :tax_type,
        :tax_authority,
        :rev_chg_flg,
        SYSTIMESTAMP,
        :partition_id,
        :ilm_dt,
        'Y',
        :batch_code,
        :batch_attempt)