CREATE OR REPLACE VIEW vw_bill_relationship AS
Select
    b.parent_bill_id     AS parent_bill_id,
    b.child_bill_id      AS child_bill_id,
    b.relationship_type  AS relationship_type,
    RPAD(b.event_id, 60) AS event_id,
    b.paid_invoice       AS paid_invoice,
    b.reuse_due_date     AS reuse_due_dt,
    b.cre_dttm           AS upload_dttm,
    'Y'                  AS extract_flg,
    null                 AS extract_dttm,
    null                 AS inv_relation_stg_id,
    b.type_cd            AS type,
    b.reason_cd          AS reason_cd,
    null                 AS tax_bseg_id
FROM
bill_relationship b
WHERE EXISTS(
    SELECT 1
    FROM outputs_registry r
    WHERE r.batch_code = b.batch_code
        AND r.batch_attempt = b.batch_attempt
        AND r.dataset_id = 'BILL'
        AND r.visible = 'Y'
);