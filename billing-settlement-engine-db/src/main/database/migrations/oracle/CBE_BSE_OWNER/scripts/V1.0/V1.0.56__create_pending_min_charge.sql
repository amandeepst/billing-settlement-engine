CREATE TABLE pending_min_charge
(
    bill_party_id       VARCHAR2(16)    NOT NULL ENABLE,
    txn_party_id        VARCHAR2(16)    NOT NULL ENABLE,
    min_chg_start_dt    DATE            NOT NULL ENABLE,
    min_chg_end_dt      DATE            NOT NULL ENABLE,
    min_chg_type        VARCHAR2(15)    NOT NULL ENABLE,
    applicable_charges  NUMBER          NOT NULL ENABLE,
    bill_dt             DATE            NOT NULL ENABLE,
    cre_dttm            TIMESTAMP       NOT NULL ENABLE,
    batch_code          VARCHAR2(128)   NOT NULL ENABLE,
    batch_attempt       NUMBER(3,0)     NOT NULL ENABLE,
    ilm_dt              TIMESTAMP       NOT NULL ENABLE,
    ilm_arch_sw         CHAR(1)         NOT NULL ENABLE,
    partition_id        INTEGER         NOT NULL ENABLE
)
    TABLESPACE CBE_BSE_DATA
    PARTITION BY RANGE (ilm_dt) INTERVAL(NUMTODSINTERVAL(1, 'DAY'))
(PARTITION before_2021 VALUES LESS THAN (TO_DATE('01-JAN-2021', 'dd-MON-yyyy')));