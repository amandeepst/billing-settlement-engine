CREATE TABLE bill_tax
(
  bill_tax_id    VARCHAR2(15)   NOT NULL ENABLE,
  bill_id        VARCHAR2(64)   NOT NULL ENABLE,
  merch_tax_reg  VARCHAR2(16)                  ,
  wp_tax_reg     VARCHAR2(16)   NOT NULL ENABLE,
  tax_type       VARCHAR2(10)                  ,
  tax_authority  VARCHAR2(10)                  ,
  rev_chg_flg    CHAR(1)                       ,
  cre_dttm       TIMESTAMP      NOT NULL ENABLE,
  batch_code     VARCHAR2(128)  NOT NULL ENABLE,
  batch_attempt  NUMBER(3,0)    NOT NULL ENABLE,
  partition_id   INTEGER        NOT NULL ENABLE,
  ilm_dt         TIMESTAMP      NOT NULL ENABLE,
  ilm_arch_sw    CHAR(1)        NOT NULL ENABLE,
  partition      NUMBER(3,0)    GENERATED ALWAYS AS (MOD(partition_id, 16)) VIRTUAL
)
TABLESPACE CBE_BSE_DATA
PARTITION BY RANGE (ilm_dt) INTERVAL(NUMTODSINTERVAL(1, 'DAY'))
  SUBPARTITION BY LIST(partition)
  SUBPARTITION TEMPLATE (
    SUBPARTITION p001 VALUES (0),
    SUBPARTITION p002 VALUES (1),
    SUBPARTITION p003 VALUES (2),
    SUBPARTITION p004 VALUES (3),
    SUBPARTITION p005 VALUES (4),
    SUBPARTITION p006 VALUES (5),
    SUBPARTITION p007 VALUES (6),
    SUBPARTITION p008 VALUES (7),
    SUBPARTITION p009 VALUES (8),
    SUBPARTITION p010 VALUES (9),
    SUBPARTITION p011 VALUES (10),
    SUBPARTITION p012 VALUES (11),
    SUBPARTITION p013 VALUES (12),
    SUBPARTITION p014 VALUES (13),
    SUBPARTITION p015 VALUES (14),
    SUBPARTITION p016 VALUES (15)
  )
  (PARTITION before_2020 VALUES LESS THAN (TO_DATE('01-NOV-2020', 'dd-MON-yyyy')))
;