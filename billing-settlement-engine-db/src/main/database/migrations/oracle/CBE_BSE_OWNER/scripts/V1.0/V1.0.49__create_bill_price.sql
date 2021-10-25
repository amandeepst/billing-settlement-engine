CREATE TABLE bill_price
(
    bill_price_id       VARCHAR2 (64)   NOT NULL ENABLE,
    party_id            VARCHAR2 (16)   NOT NULL ENABLE,
    acct_type           VARCHAR2 (15)   NOT NULL ENABLE,
    product_id          VARCHAR2 (50)   NOT NULL ENABLE,
    calc_ln_type        VARCHAR2 (30)   NOT NULL ENABLE,
    amount              NUMBER          NOT NULL ENABLE,
    currency_cd         CHAR (3)        NOT NULL ENABLE,
    bill_reference      VARCHAR2 (60)   NOT NULL ENABLE,
    invoiceable_flg     CHAR (1)        NOT NULL ENABLE,
    source_type         VARCHAR2 (30)                  ,
    source_id           VARCHAR2 (30)                  ,
    bill_ln_dtl_id      VARCHAR2 (64)   NOT NULL ENABLE,
    misc_bill_item_id   VARCHAR2 (12)   NOT NULL ENABLE,
    bill_id             VARCHAR2 (64)   NOT NULL ENABLE,
    accrued_dt          DATE            NOT NULL ENABLE,
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