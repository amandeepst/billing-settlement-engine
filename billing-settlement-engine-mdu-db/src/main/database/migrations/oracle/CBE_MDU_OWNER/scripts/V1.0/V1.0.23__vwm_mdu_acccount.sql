CREATE MATERIALIZED VIEW VWM_MDU_ACCOUNT AS
 SELECT acct1.lcp AS lcp,
      acct1.currency_cd   as currency_cd,
      acct1.acct_type     AS acct_type,
      sa1.SUB_ACCT_TYPE   AS SUB_ACCT_TYPE,
      acct1.BILL_CYC_ID   AS BILL_CYC_ID,
      per1.party_id       AS party_id,
      sa1.SUB_ACCT_ID     AS SUB_ACCT_ID,
      acct1.acct_id       AS acct_id
 FROM
      vw_sub_acct sa1 INNER JOIN vw_acct acct1 ON sa1.acct_id=acct1.acct_id
      RIGHT OUTER JOIN vw_party per1 ON per1.party_id=acct1.party_id
 WHERE acct1.status <> 'INACTIVE'
 AND  sa1.status    <> 'INACTIVE'
 AND acct1.acct_type  IN ( 'CHRG', 'FUND', 'CHBK' ,'CRWD');

CREATE INDEX VWM_MDU_ACCOUNT_IDX on VWM_MDU_ACCOUNT (PARTY_ID, ACCT_TYPE, CURRENCY_CD) PARALLEL;
ALTER INDEX VWM_MDU_ACCOUNT_IDX NOPARALLEL;
