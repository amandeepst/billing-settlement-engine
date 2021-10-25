CREATE TABLE minimum_charge_bill
(
    bill_party_id       VARCHAR2(16)    NOT NULL,
    legal_counterparty  VARCHAR2(12)    NOT NULL,
    currency            CHAR(3)         NOT NULL,
    logical_date        DATE            NOT NULL,
    cre_dttm            TIMESTAMP       NOT NULL,
    batch_code          VARCHAR2(128)   NOT NULL,
    batch_attempt       NUMBER(3,0)     NOT NULL,
    ilm_dt              TIMESTAMP       NOT NULL,
    ilm_arch_sw         CHAR(1)         NOT NULL,
    partition_id        INTEGER         NOT NULL
)
    TABLESPACE CBE_BSE_DATA
    PARTITION BY RANGE (logical_date) INTERVAL(NUMTODSINTERVAL(1, 'DAY'))
(PARTITION before_2021 VALUES LESS THAN (TO_DATE('01-JAN-2021', 'dd-MON-yyyy')));