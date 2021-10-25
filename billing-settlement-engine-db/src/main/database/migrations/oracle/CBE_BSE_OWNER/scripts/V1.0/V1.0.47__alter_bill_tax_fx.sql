DROP TABLE bill_tax_fx;

CREATE TABLE bill_tax_fx
(
    bill_tax_id     VARCHAR2(64)  NOT NULL ENABLE,
    bill_id         VARCHAR2(64)  NOT NULL ENABLE,
    tax_stat        VARCHAR2(3)   NOT NULL ENABLE,
    lcp_ccy         CHAR(3)       NOT NULL ENABLE,
    tax_lcp_ccy     CHAR(3),
    tax_net_lcp_ccy CHAR(3),
    fx_rate_id      NUMBER,
    fx_rate         NUMBER,
    cre_dttm        TIMESTAMP     NOT NULL ENABLE,
    batch_code      VARCHAR2(128) NOT NULL ENABLE,
    batch_attempt   NUMBER(3, 0)  NOT NULL ENABLE,
    partition_id    INTEGER       NOT NULL ENABLE,
    ilm_dt          TIMESTAMP     NOT NULL ENABLE,
    ilm_arch_sw     CHAR(1)       NOT NULL ENABLE,
    partition       NUMBER(3, 0) GENERATED ALWAYS AS (MOD(partition_id, 8)) VIRTUAL
)
    TABLESPACE CBE_BSE_DATA
    PARTITION BY RANGE (ilm_dt) INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
    SUBPARTITION BY LIST(partition)
    SUBPARTITION TEMPLATE (
    SUBPARTITION p001 VALUES (0, 1),
    SUBPARTITION p002 VALUES (2, 3),
    SUBPARTITION p003 VALUES (4, 5),
    SUBPARTITION p004 VALUES (6, 7)
)
(PARTITION before_2021 VALUES LESS THAN (TO_DATE('01-JAN-2021', 'dd-MON-yyyy')));