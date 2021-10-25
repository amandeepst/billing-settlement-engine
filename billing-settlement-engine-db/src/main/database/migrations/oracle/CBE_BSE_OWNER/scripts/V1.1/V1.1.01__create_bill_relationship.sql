CREATE TABLE bill_relationship
(
    parent_bill_id       VARCHAR2(64)  NOT NULL ENABLE,
    child_bill_id        VARCHAR2(64)  NOT NULL ENABLE,
    relationship_type    VARCHAR(3)    NOT NULL ENABLE,
    event_id             NUMBER(20)    NOT NULL ENABLE,
    paid_invoice         CHAR (1)      NOT NULL ENABLE,
    reuse_due_date       CHAR (1)      NOT NULL ENABLE,
    type_cd              VARCHAR2(15),
    reason_cd            VARCHAR2(256) NOT NULL ENABLE,
    cre_dttm             TIMESTAMP     NOT NULL ENABLE,
    partition_id         INTEGER       NOT NULL ENABLE,
    batch_code           VARCHAR2(128) NOT NULL ENABLE,
    batch_attempt        NUMBER        NOT NULL ENABLE,
    ilm_dt               TIMESTAMP(9)  NOT NULL ENABLE,
    ilm_arch_sw          CHAR(1)       NOT NULL ENABLE,
    partition            NUMBER        GENERATED ALWAYS AS (ORA_HASH(event_id || relationship_type, 15)) VIRTUAL,
    CONSTRAINT bill_relationship_pk PRIMARY KEY (event_id, relationship_type)
)
TABLESPACE CBE_BSE_DATA
    PARTITION BY RANGE (ilm_dt) INTERVAL(NUMTODSINTERVAL(1, 'DAY'))
SUBPARTITION BY LIST(partition)
SUBPARTITION TEMPLATE(
    SUBPARTITION p1 VALUES(0,1,2,3),
    SUBPARTITION p2 VALUES(4,5,6,7),
    SUBPARTITION p3 VALUES(8,9,10,11),
    SUBPARTITION p4 VALUES(12,13,14,15))
(PARTITION before_2021 VALUES LESS THAN (TO_DATE('01-JAN-2021', 'dd-MON-yyyy')));
