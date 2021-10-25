CREATE TABLE post_bill_adj_accounting
(
    party_id            VARCHAR2 (16)   NOT NULL ENABLE,
    acct_id             VARCHAR2 (64)   NOT NULL ENABLE,
    acct_type           VARCHAR2 (15)   NOT NULL ENABLE,
    sub_acct_id         VARCHAR2 (64)   NOT NULL ENABLE,
    sub_acct_type       VARCHAR2 (15)   NOT NULL ENABLE,
    currency_cd         CHAR (3)        NOT NULL ENABLE,
    bill_amt            NUMBER                         ,
    bill_id             VARCHAR2 (64)                  ,
    post_bill_adj_id    VARCHAR2 (12)   NOT NULL ENABLE,
    amount              NUMBER          NOT NULL ENABLE,
    lcp                 VARCHAR2 (12)   NOT NULL ENABLE,
    business_unit       VARCHAR2 (12)   NOT NULL ENABLE,
    class               VARCHAR2 (15)   NOT NULL ENABLE,
    type                VARCHAR2 (15)   NOT NULL ENABLE,
    bill_dt             DATE            NOT NULL ENABLE,
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