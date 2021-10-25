CREATE TABLE outputs_registry
(
    batch_code    VARCHAR2(128) NOT NULL ENABLE,
    batch_attempt NUMBER(3, 0)  NOT NULL ENABLE,
    ilm_dt        TIMESTAMP(9)  NOT NULL ENABLE,
    dataset_id    VARCHAR2(64)  NOT NULL ENABLE,
    logical_date  DATE          NOT NULL ENABLE,
    visible       VARCHAR2(1)   NOT NULL ENABLE,

    CONSTRAINT outputs_registry_pk PRIMARY KEY (batch_code, batch_attempt, ilm_dt, dataset_id)
);