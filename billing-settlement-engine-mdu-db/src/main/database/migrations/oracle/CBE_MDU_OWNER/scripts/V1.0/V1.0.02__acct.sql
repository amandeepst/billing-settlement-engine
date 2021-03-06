CREATE TABLE ACCT (
  ACCT_ID VARCHAR2 (30) NOT NULL,
  PARTY_ID VARCHAR2 (30) NOT NULL,
  ACCT_TYPE VARCHAR2 (10) NOT NULL,
  LCP VARCHAR2 (12) NOT NULL,
  CURRENCY_CD CHAR (3) NOT NULL,
  STATUS VARCHAR2 (15) NOT NULL,
  BILL_CYC_ID VARCHAR2 (30) NOT NULL,
  PROCESSING_GRP VARCHAR2 (15) NOT NULL,
  VALID_FROM DATE NOT NULL,
  VALID_TO DATE,
  CRE_DTTM DATE NOT NULL,
  LAST_UPD_DTTM DATE NOT NULL,
  CONSTRAINT ACCT_PARTY_FK FOREIGN KEY (PARTY_ID) REFERENCES PARTY(PARTY_ID),
  CONSTRAINT ACCT_PK PRIMARY KEY (ACCT_ID)
);