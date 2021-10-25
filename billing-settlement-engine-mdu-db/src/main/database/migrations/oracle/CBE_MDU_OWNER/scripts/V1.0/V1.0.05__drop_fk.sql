alter table ACCT drop constraint ACCT_PARTY_FK;
alter table ACCT_HIER drop constraint HIER_ACCT_PARENT_FK;
alter table ACCT_HIER drop constraint HIER_ACCT_CHILD_FK;
alter table SUB_ACCT drop constraint SUB_ACCT_ACCT_FK;