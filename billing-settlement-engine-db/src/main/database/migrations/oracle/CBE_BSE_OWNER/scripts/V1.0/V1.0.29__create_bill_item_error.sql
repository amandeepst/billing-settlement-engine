CREATE TABLE bill_item_error
(
    bill_item_id	 VARCHAR2(64)    NOT NULL ENABLE,
    first_failure_on TIMESTAMP       NOT NULL ENABLE,
    fix_dt           TIMESTAMP,
    retry_count	     NUMBER          NOT NULL ENABLE,
    code	         VARCHAR2(2048)  NOT NULL ENABLE,
    status	         CHAR(4)         NOT NULL ENABLE,
    reason	         VARCHAR2(4000)  NOT NULL ENABLE,
    stack_trace      VARCHAR2(4000),
    cre_dttm	     TIMESTAMP       NOT NULL ENABLE,
    batch_code	     VARCHAR2 (128)  NOT NULL ENABLE,
    batch_attempt	 NUMBER          NOT NULL ENABLE,
    partition_id     INTEGER         NOT NULL ENABLE,
    ilm_dt           TIMESTAMP       NOT NULL ENABLE,
    ilm_arch_sw      CHAR(1)         NOT NULL ENABLE
)
TABLESPACE CBE_BSE_DATA
    PARTITION BY RANGE (ilm_dt) INTERVAL(NUMTODSINTERVAL(1, 'DAY'))
SUBPARTITION BY hash(batch_code, batch_attempt, partition_id) SUBPARTITIONS 16 (
  PARTITION before_2020 VALUES LESS THAN (TO_DATE('01-JAN-2020', 'dd-MON-yyyy'))
);
