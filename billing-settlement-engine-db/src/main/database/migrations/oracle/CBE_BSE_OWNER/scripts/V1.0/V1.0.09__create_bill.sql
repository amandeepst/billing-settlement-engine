CREATE TABLE bill
(
  bill_id             VARCHAR2(64)  NOT NULL ENABLE,
  bill_nbr            VARCHAR2(15)  NOT NULL ENABLE,
  party_id            VARCHAR2(30)  NOT NULL ENABLE,
  bill_sub_acct_id    VARCHAR2(64)  NOT NULL ENABLE,
  tariff_type         VARCHAR2(15)                 ,
  template_type       VARCHAR2(60)                 ,
  lcp                 VARCHAR2(12)  NOT NULL ENABLE,
  acct_type           VARCHAR2(15)  NOT NULL ENABLE,
  business_unit       VARCHAR2(12)  NOT NULL ENABLE,
  bill_dt             DATE          NOT NULL ENABLE,
  bill_cyc_id         VARCHAR2(10)  NOT NULL ENABLE,
  start_dt            DATE          NOT NULL ENABLE,
  end_dt              DATE          NOT NULL ENABLE,
  currency_cd         CHAR(3)       NOT NULL ENABLE,
  bill_amt            NUMBER        NOT NULL ENABLE,
  bill_ref            VARCHAR2(30)  NOT NULL ENABLE,
  status              VARCHAR2(15)  NOT NULL ENABLE,
  adhoc_bill_flg      CHAR(1)       NOT NULL ENABLE,
  sett_sub_lvl_type   VARCHAR2(60)                 ,
  sett_sub_lvl_val    VARCHAR2(60)                 ,
  granularity         VARCHAR2(60)                 ,
  granularity_key_val VARCHAR2(60)  NOT NULL ENABLE,
  rel_waf_flg         CHAR(1)       NOT NULL ENABLE,
  rel_reserve_flg     CHAR(1)       NOT NULL ENABLE,
  fastest_pay_route   CHAR(1)       NOT NULL ENABLE,
  case_id             VARCHAR2(30)                 ,
  individual_bill     CHAR(1)       NOT NULL ENABLE,
  manual_narrative    VARCHAR2(18)  NOT NULL ENABLE,
  processing_grp      VARCHAR2(15)                 ,
  prev_bill_id        VARCHAR2(64)                 ,
  cre_dttm            TIMESTAMP     NOT NULL ENABLE,
  batch_code          VARCHAR2(128) NOT NULL ENABLE,
  batch_attempt       NUMBER(3,0)   NOT NULL ENABLE,
  partition_id        INTEGER       NOT NULL ENABLE,
  ilm_dt              TIMESTAMP     NOT NULL ENABLE,
  ilm_arch_sw         CHAR(1)       NOT NULL ENABLE,
  partition           NUMBER(3,0)   GENERATED ALWAYS AS (MOD(partition_id, 64)) VIRTUAL
)
TABLESPACE CBE_BSE_DATA
PARTITION BY RANGE (ilm_dt) INTERVAL(NUMTODSINTERVAL(1, 'DAY'))
  SUBPARTITION BY LIST(partition)
  SUBPARTITION TEMPLATE (
    SUBPARTITION p001 VALUES (0, 1, 2, 3),
    SUBPARTITION p002 VALUES (4, 5, 6, 7),
    SUBPARTITION p003 VALUES (8, 9, 10, 11),
    SUBPARTITION p004 VALUES (12, 13, 14, 15),
    SUBPARTITION p005 VALUES (16, 17, 18, 19),
    SUBPARTITION p006 VALUES (20, 21, 22, 23),
    SUBPARTITION p007 VALUES (24, 25, 26, 27),
    SUBPARTITION p008 VALUES (28, 29, 30, 31),
    SUBPARTITION p009 VALUES (32, 33, 34, 35),
    SUBPARTITION p010 VALUES (36, 37, 38, 39),
    SUBPARTITION p011 VALUES (40, 41, 42, 43),
    SUBPARTITION p012 VALUES (44, 45, 46, 47),
    SUBPARTITION p013 VALUES (48, 49, 50, 51),
    SUBPARTITION p014 VALUES (52, 53, 54, 55),
    SUBPARTITION p015 VALUES (56, 57, 58, 59),
    SUBPARTITION p016 VALUES (60, 61, 62, 63)
  )
  (PARTITION before_2020 VALUES LESS THAN (TO_DATE('01-NOV-2020', 'dd-MON-yyyy')))
;