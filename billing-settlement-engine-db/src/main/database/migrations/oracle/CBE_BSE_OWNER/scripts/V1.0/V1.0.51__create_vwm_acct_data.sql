CREATE MATERIALIZED VIEW vwm_billing_acct_data AS
    SELECT *
    FROM vw_billing_acct_data;

CREATE INDEX idx_acct_data_acct_id ON vwm_billing_acct_data ( account_id ) INITRANS 20;
