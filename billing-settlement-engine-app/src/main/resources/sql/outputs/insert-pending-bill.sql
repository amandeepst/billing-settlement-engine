INSERT /*+ :hints */ INTO pending_bill (bill_id,
                                    party_id,
                                    lcp,
                                    acct_id,
                                    bill_sub_acct_id,
                                    acct_type,
                                    business_unit,
                                    bill_cyc_id,
                                    start_dt,
                                    end_dt,
                                    currency_cd,
                                    bill_ref,
                                    adhoc_bill,
                                    sett_sub_lvl_type,
                                    sett_sub_lvl_val,
                                    granularity,
                                    granularity_key_val,
                                    debt_dt,
                                    debt_mig_type,
                                    overpayment_flg,
                                    rel_waf_flg,
                                    rel_reserve_flg,
                                    fastest_pay_route,
                                    case_id,
                                    individual_bill,
                                    manual_narrative,
                                    miscalculation_flag,
                                    cre_dttm,
                                    batch_code,
                                    batch_attempt,
                                    partition_id,
                                    ilm_dt,
                                    ilm_arch_sw,
                                    first_failure_on,
                                    retry_count)
VALUES (:bill_id,
        :party_id,
        :lcp,
        :acct_id,
        :bill_sub_acct_id,
        :acct_type,
        :business_unit,
        :bill_cyc_id,
        :start_dt,
        :end_dt,
        :currency_cd,
        :bill_ref,
        :adhoc_bill,
        :sett_sub_lvl_type,
        :sett_sub_lvl_val,
        :granularity,
        :granularity_key_val,
        :debt_dt,
        :debt_mig_type,
        :overpayment_flg,
        :rel_waf_flg,
        :rel_reserve_flg,
        :fastest_pay_route,
        :case_id,
        :individual_bill,
        :manual_narrative,
        :miscalculation_flag,
        SYSTIMESTAMP,
        :batch_code,
        :batch_attempt,
        :partition_id,
        :ilm_dt,
        'Y',
        :first_failure_on,
        :retry_count)