CREATE TABLE bill_accounting
(
  party_id           VARCHAR2(16)   NOT NULL ENABLE,
  acct_id            VARCHAR2(64)   NOT NULL ENABLE,
  acct_type          VARCHAR2(15)   NOT NULL ENABLE,
  bill_sub_acct_id   VARCHAR2(64)   NOT NULL ENABLE,
  sub_acct_type      VARCHAR2(15)   NOT NULL ENABLE,
  currency_cd        CHAR(3)        NOT NULL ENABLE,
  bill_amt           NUMBER         NOT NULL ENABLE,
  bill_id            VARCHAR2(64)   NOT NULL ENABLE,
  bill_ln_hash       VARCHAR2(256)                 ,
  bill_ln_id         VARCHAR2(64)   NOT NULL ENABLE,
  total_line_amount  NUMBER         NOT NULL ENABLE,
  lcp                VARCHAR2(12)   NOT NULL ENABLE,
  business_unit      VARCHAR2(12)   NOT NULL ENABLE,
  class              VARCHAR2(15)   NOT NULL ENABLE,
  calc_ln_id         VARCHAR2(64)   NOT NULL ENABLE,
  type               VARCHAR2(15)   NOT NULL ENABLE,
  calc_ln_type       VARCHAR2(15)                  ,
  calc_line_amount   NUMBER         NOT NULL ENABLE,
  bill_dt            DATE           NOT NULL ENABLE,
  cre_dttm           TIMESTAMP      NOT NULL ENABLE,
  batch_code         VARCHAR2(128)  NOT NULL ENABLE,
  batch_attempt      NUMBER(3,0)    NOT NULL ENABLE,
  partition_id       INTEGER        NOT NULL ENABLE,
  ilm_dt             TIMESTAMP      NOT NULL ENABLE,
  ilm_arch_sw        CHAR(1)        NOT NULL ENABLE,
  partition          NUMBER(3,0)    GENERATED ALWAYS AS (MOD(partition_id, 256)) VIRTUAL
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
    SUBPARTITION p016 VALUES (60, 61, 62, 63),
    SUBPARTITION p017 VALUES (64, 65, 66, 67),
    SUBPARTITION p018 VALUES (68, 69, 70, 71),
    SUBPARTITION p019 VALUES (72, 73, 74, 75),
    SUBPARTITION p020 VALUES (76, 77, 78, 79),
    SUBPARTITION p021 VALUES (80, 81, 82, 83),
    SUBPARTITION p022 VALUES (84, 85, 86, 87),
    SUBPARTITION p023 VALUES (88, 89, 90, 91),
    SUBPARTITION p024 VALUES (92, 93, 94, 95),
    SUBPARTITION p025 VALUES (96, 97, 98, 99),
    SUBPARTITION p026 VALUES (100, 101, 102, 103),
    SUBPARTITION p027 VALUES (104, 105, 106, 107),
    SUBPARTITION p028 VALUES (108, 109, 110, 111),
    SUBPARTITION p029 VALUES (112, 113, 114, 115),
    SUBPARTITION p030 VALUES (116, 117, 118, 119),
    SUBPARTITION p031 VALUES (120, 121, 122, 123),
    SUBPARTITION p032 VALUES (124, 125, 126, 127),
    SUBPARTITION p033 VALUES (128, 129, 130, 131),
    SUBPARTITION p034 VALUES (132, 133, 134, 135),
    SUBPARTITION p035 VALUES (136, 137, 138, 139),
    SUBPARTITION p036 VALUES (140, 141, 142, 143),
    SUBPARTITION p037 VALUES (144, 145, 146, 147),
    SUBPARTITION p038 VALUES (148, 149, 150, 151),
    SUBPARTITION p039 VALUES (152, 153, 154, 155),
    SUBPARTITION p040 VALUES (156, 157, 158, 159),
    SUBPARTITION p041 VALUES (160, 161, 162, 163),
    SUBPARTITION p042 VALUES (164, 165, 166, 167),
    SUBPARTITION p043 VALUES (168, 169, 170, 171),
    SUBPARTITION p044 VALUES (172, 173, 174, 175),
    SUBPARTITION p045 VALUES (176, 177, 178, 179),
    SUBPARTITION p046 VALUES (180, 181, 182, 183),
    SUBPARTITION p047 VALUES (184, 185, 186, 187),
    SUBPARTITION p048 VALUES (188, 189, 190, 191),
    SUBPARTITION p049 VALUES (192, 193, 194, 195),
    SUBPARTITION p050 VALUES (196, 197, 198, 199),
    SUBPARTITION p051 VALUES (200, 201, 202, 203),
    SUBPARTITION p052 VALUES (204, 205, 206, 207),
    SUBPARTITION p053 VALUES (208, 209, 210, 211),
    SUBPARTITION p054 VALUES (212, 213, 214, 215),
    SUBPARTITION p055 VALUES (216, 217, 218, 219),
    SUBPARTITION p056 VALUES (220, 221, 222, 223),
    SUBPARTITION p057 VALUES (224, 225, 226, 227),
    SUBPARTITION p058 VALUES (228, 229, 230, 231),
    SUBPARTITION p059 VALUES (232, 233, 234, 235),
    SUBPARTITION p060 VALUES (236, 237, 238, 239),
    SUBPARTITION p061 VALUES (240, 241, 242, 243),
    SUBPARTITION p062 VALUES (244, 245, 246, 247),
    SUBPARTITION p063 VALUES (248, 249, 250, 251),
    SUBPARTITION p064 VALUES (252, 253, 254, 255)
  )
  (PARTITION before_2020 VALUES LESS THAN (TO_DATE('01-NOV-2020', 'dd-MON-yyyy')))
;