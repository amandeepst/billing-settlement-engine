CREATE INDEX IDX_BILL_SVC_QTY_BILL_ID ON bill_line_svc_qty(BILL_ID) LOCAL INITRANS 20 PARALLEL;
ALTER INDEX IDX_BILL_SVC_QTY_BILL_ID NOPARALLEL;