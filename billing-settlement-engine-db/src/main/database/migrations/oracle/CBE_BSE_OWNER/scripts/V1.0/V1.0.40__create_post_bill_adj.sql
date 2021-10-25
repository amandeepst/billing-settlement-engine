CREATE TABLE post_bill_adj
(
    post_bill_adj_id    VARCHAR2 (12)   NOT NULL ENABLE,
    post_bill_adj_type  VARCHAR2 (8)    NOT NULL ENABLE,
    post_bill_adj_descr VARCHAR2 (60)                  ,
    source_id           VARCHAR2 (30)                  ,
    bill_id             VARCHAR2 (64)   NOT NULL ENABLE,
    amount              NUMBER          NOT NULL ENABLE,
    currency_cd         CHAR (3)        NOT NULL ENABLE,
    bill_sub_acct_id    VARCHAR2 (30)   NOT NULL ENABLE,
    rel_sub_acct_id     VARCHAR2 (30)                  ,
    cre_dttm            TIMESTAMP       NOT NULL ENABLE,
    batch_code          VARCHAR2(128)   NOT NULL ENABLE,
    batch_attempt       NUMBER(3,0)     NOT NULL ENABLE,
    ilm_dt              TIMESTAMP       NOT NULL ENABLE,
    ilm_arch_sw         CHAR(1)         NOT NULL ENABLE,
    partition_id        INTEGER         NOT NULL ENABLE
)
    TABLESPACE CBE_BSE_DATA
    PARTITION BY RANGE (ilm_dt) INTERVAL(NUMTODSINTERVAL(1, 'DAY'))
(PARTITION before_2020 VALUES LESS THAN (TO_DATE('01-NOV-2020', 'dd-MON-yyyy')));