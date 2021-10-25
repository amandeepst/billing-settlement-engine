CREATE TABLE batch_seed (
   run_id         NUMBER             GENERATED ALWAYS AS IDENTITY START WITH 1 NOCACHE ORDER NOT NULL ENABLE,
   batch_code     VARCHAR2(128)      NOT NULL ENABLE,
   batch_attempt  NUMBER(3, 0)       NOT NULL ENABLE,
   created_at     TIMESTAMP          DEFAULT SYSTIMESTAMP NOT NULL,
   CONSTRAINT     batch_seed_bse_pk  PRIMARY KEY (run_id)
);
