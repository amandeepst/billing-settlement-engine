CREATE TABLE bill_error
(
    bill_id          VARCHAR2(64)  NOT NULL ENABLE,
    bill_dt          DATE          NOT NULL ENABLE,
    cre_dttm         TIMESTAMP     NOT NULL ENABLE,
    first_failure_on TIMESTAMP     NOT NULL ENABLE,
    fix_dt           TIMESTAMP,
    retry_count      NUMBER,
    partition_id     NUMBER        NOT NULL ENABLE,
    ilm_dt           TIMESTAMP     NOT NULL ENABLE,
    ilm_arch_sw      CHAR(1)       NOT NULL ENABLE,
    batch_code       VARCHAR2(128) NOT NULL ENABLE,
    batch_attempt    NUMBER        NOT NULL ENABLE
)
    TABLESPACE CBE_BSE_DATA
    PARTITION BY RANGE (ilm_dt) INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
    SUBPARTITION BY hash (batch_code, batch_attempt, partition_id) SUBPARTITIONS 16
(
    PARTITION before_2021 VALUES LESS THAN (TO_DATE('01-JAN-2021', 'dd-MON-yyyy'))
);
