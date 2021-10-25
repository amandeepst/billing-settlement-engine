CREATE MATERIALIZED VIEW VWM_BIL_EQUIV_NON_TRANS_PRICE AS
WITH current_data AS (
    SELECT
        trans.per_id_nbr,
        trim(trans.priceitem_cd) as priceitem_cdt,
        trans.currency_cd,
        COUNT(1) AS count,
        SUM(trans.calc_amt) AS total_charge,
        TRUNC(trans.ilm_dt)-1 AS accrued_dt,
        decode(COUNT(1),
            1, MIN(id_map.bill_id),
            2, MIN(id_map.bill_id) || ' | ' || MAX(id_map.bill_id),
            MIN(id_map.bill_id) || ' | ' || MAX(id_map.bill_id) || ' | + ' || (COUNT(1)-2) || ' others') AS bill_ids
    FROM
        cm_non_trans_price_bak trans
        inner join CM_BILL_ID_MAP_BAK id_map on trans.bill_reference = id_map.bill_reference
    WHERE
        TRUNC(trans.ILM_DT)-1 BETWEEN '01-DEC-20' AND '31-DEC-20'
    GROUP BY
        trans.per_id_nbr,
        trim(trans.priceitem_cd),
        trans.currency_cd,
        TRUNC(trans.ilm_dt)-1
), new_data AS (
    SELECT
        trans.per_id_nbr,
        trans.priceitem_cd,
        trans.currency_cd,
        COUNT(1) AS count,
        SUM(trans.calc_amt) AS total_charge,
        trans.accrued_dt,
        listagg(bill.bill_id, ' | ') WITHIN GROUP (ORDER BY bill.bill_id) AS bill_ids
    FROM
        CBE_BSE_OWNER.VW_NON_TRANS_PRICE trans INNER JOIN CBE_BSE_OWNER.bill bill on trans.bill_reference = bill.bill_ref
    GROUP BY
        trans.per_id_nbr,
        trans.priceitem_cd,
        trans.currency_cd,
        trans.accrued_dt
)
SELECT
    old.per_id_nbr,
    old.priceitem_cdt,
    old.currency_cd,
    old.accrued_dt,
    old.count as old_count,
    new.count as new_count,
    old.total_charge as old_total_charge,
    new.total_charge as new_total_charge,
    old.bill_ids as old_bill_ids,
    new.bill_ids as new_bill_ids
FROM
    current_data old
    LEFT OUTER JOIN new_data new ON old.per_id_nbr = new.per_id_nbr
                                    AND old.priceitem_cdt = new.priceitem_cd
                                    AND old.currency_cd = new.currency_cd
                                    AND old.accrued_dt = new.accrued_dt
WHERE
    (
    old.count <> new.count
    OR
    old.total_charge <> new.total_charge
    );