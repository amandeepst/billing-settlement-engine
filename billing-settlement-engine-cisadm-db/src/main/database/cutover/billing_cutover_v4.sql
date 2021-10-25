
SET SERVEROUTPUT ON;

ALTER SESSION ENABLE PARALLEL DML;

ALTER TABLE cbe_cue_owner.cm_misc_bill_item ENABLE ROW MOVEMENT;

ALTER TABLE cbe_cue_owner.cm_misc_bill_item_ln ENABLE ROW MOVEMENT;


DECLARE
    v_bill_seq   NUMBER;
BEGIN
    SELECT MAX(alt_bill_id) + 1
    INTO v_bill_seq
    FROM cisadm.cm_invoice_data_bak
    WHERE ilm_dt = (
        SELECT MAX(ilm_dt)
        FROM cisadm.cm_invoice_data
    );

    EXECUTE IMMEDIATE 'ALTER SEQUENCE CBE_BSE_OWNER.BILL_NO_SQ RESTART START WITH ' || v_bill_seq
	;
END;
/

DECLARE
    v_bill_map_seq   NUMBER;
BEGIN
    SELECT MAX(bill_map_id) + 1
    INTO v_bill_seq
    FROM cisadm.cm_bill_id_map_bak
    WHERE ilm_dt >= (
        SELECT trunc(MAX(ilm_dt))
        FROM cisadm.cm_bill_id_map_bak
    );

    EXECUTE IMMEDIATE 'ALTER SEQUENCE CBE_BSE_OWNER.BILL_SOURCE_ID_SQ RESTART START WITH ' || v_bill_map_seq
	;
END;
/


BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE CBE_BSE_OWNER.TEMP_CUTOVER_BCHG_PCE'
    ;
EXCEPTION
    WHEN OTHERS THEN
        IF sqlcode = -00942 THEN
            dbms_output.put_line('Ignore Error - ' || sqlerrm);
        ELSE
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE CBE_BSE_OWNER.TEMP_CUTOVER_BCHG_CUE'
    ;
EXCEPTION
    WHEN OTHERS THEN
        IF sqlcode = -00942 THEN
            dbms_output.put_line('Ignore Error - ' || sqlerrm);
        ELSE
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE CBE_BSE_OWNER.TEMP_CUTOVER_BCHG_PCE
     (
          BILLABLE_CHG_ID             CHAR(12),
          ILM_DT                      TIMESTAMP(6),
		  OFFSET					  NUMBER	
     )'
    ;
EXCEPTION
    WHEN OTHERS THEN
        IF sqlcode = -00955 THEN
            dbms_output.put_line('Ignore Error - ' || sqlerrm);
        ELSE
            RAISE;
        END IF;
END;
/


BEGIN
    EXECUTE IMMEDIATE 'create unique index idx_tmp_bchg
        on CBE_BSE_OWNER.TEMP_CUTOVER_BCHG_PCE (billable_chg_id, ilm_dt)'
    ;
EXCEPTION
    WHEN OTHERS THEN
        IF sqlcode = -00955 THEN
            dbms_output.put_line('Ignore Error - ' || sqlerrm);
        ELSE
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE CBE_BSE_OWNER.TEMP_CUTOVER_BCHG_CUE
     (
          BILLABLE_CHG_ID             CHAR(12),
          ILM_DT                      TIMESTAMP(6)
     )'
    ;
EXCEPTION
    WHEN OTHERS THEN
        IF sqlcode = -00955 THEN
            dbms_output.put_line('Ignore Error - ' || sqlerrm);
        ELSE
            RAISE;
        END IF;
END;
/

DECLARE
    v_ilm_dt   TIMESTAMP := SYSDATE;
    v_hwm      TIMESTAMP;
BEGIN
    SELECT MAX(watermark_high)
    INTO v_hwm
    FROM (
        SELECT MAX(watermark_high) AS watermark_high
        FROM cbe_pce_owner.batch_history
        WHERE state = 'COMPLETED'
        UNION ALL
        SELECT MAX(watermark_high) AS watermark_high
        FROM cbe_cue_owner.batch_history
        WHERE state = 'COMPLETED'
    );

    INSERT /*+parallel (32) */ INTO cbe_bse_owner.temp_cutover_bchg_pce
        ( SELECT /*+parallel (32) */ billable_chg_id,
                 ilm_dt,
                 ora_hash(TO_CHAR(ilm_dt, 'YYYYMMDD'), 3) + 1 AS offset
        FROM cbe_pce_owner.vwm_billable_charge a
        WHERE EXISTS (
            SELECT 1
            FROM cisadm.ci_bseg_calc b,
                 cisadm.ci_bseg c,
                 cisadm.ci_bill d
            WHERE a.billable_chg_id = b.billable_chg_id
                  AND b.bseg_id = c.bseg_id
                  AND c.bill_id = d.bill_id
                  AND d.bill_stat_flg = 'P'
        )
        );

    dbms_output.put_line('PCE Billable charge temp table insert complete. '
                         || SQL%rowcount
                         || ' rows inserted');
    INSERT INTO cbe_bse_owner.temp_cutover_bchg_cue
        ( SELECT /*+ parallel (32) */ misc_bill_item_id,
                 ilm_dt
        FROM cbe_cue_owner.vw_misc_bill_item a
        WHERE EXISTS (
            SELECT 1
            FROM cisadm.ci_bseg_calc b,
                 cisadm.ci_bseg c,
                 cisadm.ci_bill d
            WHERE a.misc_bill_item_id = b.billable_chg_id
                  AND b.bseg_id = c.bseg_id
                  AND c.bill_id = d.bill_id
                  AND d.bill_stat_flg = 'P'
        )
        );

    dbms_output.put_line('CUE Billable charge temp table insert complete. '
                         || SQL%rowcount
                         || ' rows inserted');
    INSERT INTO cbe_bse_owner.batch_history (
        batch_code,
        attempt,
        state,
        watermark_low,
        watermark_high,
        comments,
        metadata,
        created_at
    ) VALUES (
        'BASELINE_CUTOVER',
        1,
        'COMPLETED',
        v_hwm,
        v_hwm,
        'Baseline insert for billing',
        NULL,
        SYSDATE
    );

    dbms_output.put_line('BSE batch history insert complete. '
                         || SQL%rowcount
                         || ' rows inserted');
    INSERT INTO cbe_pce_owner.batch_history (
        batch_code,
        attempt,
        state,
        watermark_low,
        watermark_high,
        comments,
        metadata,
        created_at
    ) VALUES (
        'BILLING_CUTOVER_PCE',
        1,
        'COMPLETED',
        v_hwm,
        v_ilm_dt,
        'Cutover insert for billing',
        NULL,
        SYSDATE
    );

    dbms_output.put_line('PCE batch history insert complete. '
                         || SQL%rowcount
                         || ' rows inserted');
    INSERT INTO cbe_pce_owner.outputs_registry (
        batch_code,
        batch_attempt,
        ilm_dt,
        dataset_id,
        logical_date,
        visible
    ) VALUES (
        'BILLING_CUTOVER_PCE',
        1,
        v_ilm_dt,
        'BILLABLE_CHARGE',
        v_ilm_dt,
        'Y'
    );

    dbms_output.put_line('PCE outputs registry insert complete. '
                         || SQL%rowcount
                         || ' rows inserted');
    COMMIT;
    INSERT /* + parallel (32) */ INTO cbe_pce_owner.billable_charge (
        billable_item_id,
        billable_charge_id,
        sub_account_id,
        start_dt,
        priceitem_cd,
        price_asgn_id,
        settlement_level_type,
        settlement_granularity,
        currency_from_scheme,
        funding_currency,
        merchant_amount_signage,
        sett_level_granularity,
        child_product,
        parent_acct_id,
        cre_dttm,
        batch_code,
        batch_attempt,
        ilm_dt,
        ilm_arch_sw,
        aggregation_hash,
        billing_currency,
        legal_counterparty,
        price_currency,
        txn_currency,
        product_class,
        partition_id
    )
        ( SELECT /*+ parallel (32) */ billable_item_id,
                 billable_charge_id,
                 sub_account_id,
                 start_dt,
                 priceitem_cd,
                 price_asgn_id,
                 settlement_level_type,
                 settlement_granularity,
                 currency_from_scheme,
                 funding_currency,
                 merchant_amount_signage,
                 sett_level_granularity,
                 child_product,
                 parent_acct_id,
                 SYSDATE   AS cre_dttm,
                 'BILLING_CUTOVER_PCE' AS batch_code,
                 1 AS batch_attempt,
                 v_hwm + numtodsinterval(b.offset, 'hour') AS ilm_dt,
                 ilm_arch_sw,
                 aggregation_hash,
                 billing_currency,
                 legal_counterparty,
                 price_currency,
                 txn_currency,
                 product_class,
                 partition_id
        FROM cbe_pce_owner.billable_charge a
        INNER JOIN cbe_bse_owner.temp_cutover_bchg_pce b ON a.billable_charge_id = b.billable_chg_id
                                                            AND a.ilm_dt = b.ilm_dt
        WHERE EXISTS (
            SELECT 1
            FROM cbe_pce_owner.outputs_registry r
            WHERE r.batch_code = a.batch_code
                  AND r.batch_attempt = a.batch_attempt
                  AND r.dataset_id = 'BILLABLE_CHARGE'
                  AND r.visible = 'Y'
        )
        );
		
	

    dbms_output.put_line('PCE billable charge insert complete. '
                         || SQL%rowcount
                         || ' rows inserted');
						 
	COMMIT;						
						 
    INSERT /*+ parallel (32) */ INTO cbe_pce_owner.billable_charge_line (
        billable_item_id,
        billable_charge_id,
        line_sequence,
        charge_amount,
        currency_cd,
        distribution_id,
        precise_charge_amount,
        batch_code,
        batch_attempt,
        ilm_dt,
        ilm_arch_sw,
        description_on_bill,
        characteristic_value,
        rate,
        rate_type,
        partition_id
    )
        ( SELECT /*+ parallel (32) */ billable_item_id,
                 billable_charge_id,
                 line_sequence,
                 charge_amount,
                 currency_cd,
                 distribution_id,
                 precise_charge_amount,
                 'BILLING_CUTOVER_PCE' AS batch_code,
                 1 AS batch_attempt,
                 v_hwm + numtodsinterval(b.offset, 'hour') AS ilm_dt,
                 ilm_arch_sw,
                 description_on_bill,
                 characteristic_value,
                 rate,
                 rate_type,
                 partition_id
        FROM cbe_pce_owner.billable_charge_line a
        INNER JOIN cbe_bse_owner.temp_cutover_bchg_pce b ON a.billable_charge_id = b.billable_chg_id
                                                            AND a.ilm_dt = b.ilm_dt
        WHERE EXISTS (
            SELECT 1
            FROM cbe_pce_owner.outputs_registry r
            WHERE r.batch_code = a.batch_code
                  AND r.batch_attempt = a.batch_attempt
                  AND r.dataset_id = 'BILLABLE_CHARGE'
                  AND r.visible = 'Y'
        )
        );

    dbms_output.put_line('PCE billable charge line insert complete. '
                         || SQL%rowcount
                         || ' rows inserted');
						 
	COMMIT;
						 
    INSERT /*+ parallel (32) */ INTO cbe_pce_owner.billable_charge_service_output (
        billable_item_id,
        billable_charge_id,
        sqi_cd,
        rate_schedule,
        svc_qty,
        batch_code,
        batch_attempt,
        ilm_dt,
        ilm_arch_sw,
        partition_id
    )
        ( SELECT /*+ parallel (32) */ billable_item_id,
                 billable_charge_id,
                 sqi_cd,
                 rate_schedule,
                 svc_qty,
                 'BILLING_CUTOVER_PCE' AS batch_code,
                 1 AS batch_attempt,
                 v_hwm + numtodsinterval(b.offset, 'hour') AS ilm_dt,
                 ilm_arch_sw,
                 partition_id
        FROM cbe_pce_owner.billable_charge_service_output a
        INNER JOIN cbe_bse_owner.temp_cutover_bchg_pce b ON a.billable_charge_id = b.billable_chg_id
                                                            AND a.ilm_dt = b.ilm_dt
        WHERE EXISTS (
            SELECT 1
            FROM cbe_pce_owner.outputs_registry r
            WHERE r.batch_code = a.batch_code
                  AND r.batch_attempt = a.batch_attempt
                  AND r.dataset_id = 'BILLABLE_CHARGE'
                  AND r.visible = 'Y'
        )
        );

    dbms_output.put_line('PCE billable charge service output insert complete. '
                         || SQL%rowcount
                         || ' rows inserted');
	
	COMMIT;						 
						 
    UPDATE /*+parallel (12) */ cbe_cue_owner.cm_misc_bill_item a
    SET
        ilm_dt = v_ilm_dt
    WHERE EXISTS (
        SELECT 1
        FROM cbe_bse_owner.temp_cutover_bchg_cue b
        WHERE a.misc_bill_item_id = b.billable_chg_id
              AND a.ilm_dt = b.ilm_dt
    );

    dbms_output.put_line('CUE misc bill item update complete. '
                         || SQL%rowcount
                         || ' rows updated');
						 
	COMMIT;
						 
    UPDATE /*+parallel (12) */ cbe_cue_owner.cm_misc_bill_item_ln a
    SET
        ilm_dt = v_ilm_dt
    WHERE EXISTS (
        SELECT 1
        FROM cbe_bse_owner.temp_cutover_bchg_cue b
        WHERE a.bill_item_id = b.billable_chg_id
              AND a.ilm_dt = b.ilm_dt
    );

    dbms_output.put_line('CUE misc bill item line update complete. '
                         || SQL%rowcount
                         || ' rows updated');
						 
	COMMIT;
						 
    INSERT INTO cbe_bse_owner.pending_min_charge (
        bill_party_id,
        txn_party_id,
        min_chg_start_dt,
        min_chg_end_dt,
        min_chg_type,
        applicable_charges,
        bill_dt,
        cre_dttm,
        batch_code,
        batch_attempt,
        ilm_dt,
        ilm_arch_sw,
        partition_id,
        legal_counterparty,
        currency
    )
        ( SELECT /*+ parallel (32) */ invdt.billing_party_id   AS bill_party_id,
                 ln.billing_party_id      AS txn_party_id,
                 TO_DATE('01-JUN-21 00.00.00', 'DD-MON-YY HH24.MI.SS') AS min_chg_start_dt,
                 TO_DATE('30-JUN-21 00.00.00', 'DD-MON-YY HH24.MI.SS') AS min_chg_end_dt,
                 'MIN_P_CHRG',
                 SUM(bcl.calc_amt) AS applicable_charges,
                 TO_DATE('30-JUN-21 00.00.00', 'DD-MON-YY HH24.MI.SS') AS bill_dt,
                 SYSDATE                  AS cre_dttm,
                 'BASELINE_CUTOVER' AS batch_code,
                 1 AS batch_attempt,
                 v_ilm_dt                 AS ilm_dt,
                 'Y' AS ilm_arch_sw,
                 1 AS partition_id,
                 'PO11000' || invdt.cis_division AS legal_counterparty,
                 invdt.currency_cd        AS currency
        FROM cisadm.cm_invoice_data invdt
        INNER JOIN cisadm.cm_invoice_data_ln ln ON invdt.bill_id = ln.bill_id
        INNER JOIN cisadm.cm_inv_data_ln_bcl bcl ON ln.bseg_id = bcl.bseg_id
        INNER JOIN cisadm.ci_priceitem_rel prel ON TRIM(ln.price_category) = TRIM(prel.priceitem_chld_cd)
        WHERE 1 = 1
              AND invdt.ilm_dt >= TO_DATE('02-JUN-21 00.00.00', 'DD-MON-YY HH24.MI.SS')
              AND ln.ilm_dt >= TO_DATE('02-JUN-21 00.00.00', 'DD-MON-YY HH24.MI.SS')
              AND bcl.ilm_dt >= TO_DATE('02-JUN-21 00.00.00', 'DD-MON-YY HH24.MI.SS')
              AND bcl_type <> 'F_M_AMT'
              AND prel.priceitem_par_cd = 'MINCHRGP'
              AND invdt.bill_cyc_cd = 'WPDY'
        GROUP BY invdt.billing_party_id,
                 ln.billing_party_id,
                 TO_DATE('01-JUN-21 00.00.00', 'DD-MON-YY HH24.MI.SS'),
                 TO_DATE('30-JUN-21 00.00.00', 'DD-MON-YY HH24.MI.SS'),
                 'MIN_P_CHRG',
                 TO_DATE('30-JUN-21 00.00.00', 'DD-MON-YY HH24.MI.SS'),
                 SYSDATE,
                 v_ilm_dt,
                 'Y',
                 1,
                 'PO11000' || invdt.cis_division,
                 invdt.currency_cd
        HAVING SUM(bcl.calc_amt) <> 0
        );

    dbms_output.put_line('Pending minimum charges insert complete. '
                         || SQL%rowcount
                         || ' rows updated');
						 
	COMMIT;
						 
    UPDATE /*+parallel (32) */ cbe_mam_owner.cm_merch_acct_ledger_balance
    SET
        batch_attempt = 0
    WHERE sub_account_type IN (
        'CHRG    ',
        'RECR    '
    )
          AND ilm_dt = (
        SELECT MAX(ilm_dt)
        FROM cbe_mam_owner.cm_merch_acct_ledger_balance
    );

    dbms_output.put_line('Merchant balance update. '
                         || SQL%rowcount
                         || ' rows updated');
						 
	COMMIT;
						 
    INSERT /*+parallel (32) */ INTO cbe_mam_owner.cm_merch_acct_ledger_balance (
        party_id,
        legal_counterparty,
        currency,
        account_type,
        account_balance,
        sub_account_type,
        sub_account_balance,
        cre_dttm,
        batch_code,
        batch_attempt,
        partition_id,
        ilm_dt,
        ilm_arch_sw,
        logical_dt
    )
        ( SELECT /*+parallel (32) */ party_id,
                 legal_counterparty,
                 currency,
                 account_type,
                 account_balance,
                 'CHRG' AS sub_account_type,
                 SUM(sub_account_balance) AS sub_account_balance,
                 cre_dttm,
                 batch_code,
                 1 AS batch_attempt,
                 partition_id,
                 ilm_dt,
                 ilm_arch_sw,
                 logical_dt
        FROM cbe_mam_owner.cm_merch_acct_ledger_balance
        WHERE ilm_dt = (
            SELECT MAX(ilm_dt)
            FROM cbe_mam_owner.cm_merch_acct_ledger_balance
        )
              AND sub_account_type IN (
            'CHRG    ',
            'RECR    '
        )
        GROUP BY party_id,
                 legal_counterparty,
                 currency,
                 account_type,
                 account_balance,
                 'CHRG',
                 cre_dttm,
                 batch_code,
                 1,
                 partition_id,
                 ilm_dt,
                 ilm_arch_sw,
                 logical_dt
        );

    dbms_output.put_line('Merchant balance RECR to CHRG migration complete. '
                         || SQL%rowcount
                         || ' rows inserted');
	
	COMMIT;
	
    dbms_output.put_line('All changes committed');
	
EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('Script failed: ' || sqlerrm);
        ROLLBACK;
        dbms_output.put_line('All uncommited changes rolled back.');
END;
/

ALTER TABLE cbe_cue_owner.cm_misc_bill_item DISABLE ROW MOVEMENT;

ALTER TABLE cbe_cue_owner.cm_misc_bill_item_ln DISABLE ROW MOVEMENT;

