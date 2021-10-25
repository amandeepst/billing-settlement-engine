INSERT INTO bill_line_calc (bill_line_calc_id,
                            bill_ln_id,
                            bill_id,
                            calc_ln_class,
                            calc_ln_type,
                            calc_ln_type_descr,
                            amount,
                            include_on_bill,
                            rate_type,
                            rate_val,
                            tax_stat,
                            tax_rate,
                            tax_stat_descr,
                            cre_dttm,
                            batch_code,
                            batch_attempt,
                            partition_id,
                            ilm_dt,
                            ilm_arch_sw)
VALUES (:bill_line_calc_id,
        :bill_ln_id,
        :bill_id,
        :calc_ln_class,
        :calc_ln_type,
        :calc_ln_type_descr,
        :amount,
        :include_on_bill,
        :rate_type,
        :rate_val,
        :tax_stat,
        :tax_rate,
        :tax_stat_descr,
        SYSDATE,
        :batch_code,
        :batch_attempt,
        :partition_id,
        :ilm_dt,
        'Y')