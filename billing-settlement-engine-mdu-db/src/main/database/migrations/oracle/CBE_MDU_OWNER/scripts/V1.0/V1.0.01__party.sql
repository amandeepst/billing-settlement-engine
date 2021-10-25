CREATE TABLE PARTY (
  PARTY_ID VARCHAR2 (30) NOT NULL,
  COUNTRY_CD CHAR (3)  NOT NULL,
  STATE VARCHAR2 (6),
  BUSINESS_UNIT VARCHAR2 (12) NOT NULL,
  MERCH_TAX_REG VARCHAR2 (16),
  VALID_FROM DATE NOT NULL,
  VALID_TO DATE,
  CRE_DTTM DATE NOT NULL,
  LAST_UPD_DTTM DATE NOT NULL,
  CONSTRAINT PARTY_PK PRIMARY KEY(PARTY_ID)
);