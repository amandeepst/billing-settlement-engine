CREATE TABLE bill_line_transposition (
    bill_id            VARCHAR2(64)    NOT NULL ENABLE,
    bill_ln_id         VARCHAR2(64)    NOT NULL ENABLE,
    bill_line_party_id VARCHAR2(30)    NOT NULL ENABLE,
    class              VARCHAR2(30)    NOT NULL ENABLE,
    product_id         VARCHAR2(50)    NOT NULL ENABLE,
    product_descr      VARCHAR2(60)    NOT NULL ENABLE,
    price_ccy          CHAR(3),
    fund_ccy           CHAR(3),
    qty                NUMBER          NOT NULL ENABLE,
    msc_pi             NUMBER,
    msc_pc             NUMBER,
    asf_pi             NUMBER,
    asf_pc             NUMBER,
    pi_amt             NUMBER,
    pc_amt             NUMBER,
    fund_amt           NUMBER,
    ic_amt             NUMBER,
    sf_amt             NUMBER,
    total_amt          NUMBER          NOT NULL ENABLE,
    tax_stat           VARCHAR2(3),
    cre_dttm           DATE            NOT NULL ENABLE,
    partition_id       NUMBER          NOT NULL ENABLE,
    ilm_dt             DATE            NOT NULL ENABLE,
    ilm_arch_sw        CHAR(1)         NOT NULL ENABLE,
    batch_code         VARCHAR2(128)   NOT NULL ENABLE,
    batch_attempt      NUMBER          NOT NULL ENABLE,
    partition          NUMBER(3,0)     GENERATED ALWAYS AS (MOD(partition_id, 128)) VIRTUAL
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
        SUBPARTITION p032 VALUES (124, 125, 126, 127)
    )
(PARTITION before_2020 VALUES LESS THAN (TO_DATE('01-NOV-2020', 'dd-MON-yyyy')))
;

