CREATE INDEX IDX_BILL_LINE_CALC_BILL_ID ON bill_line_calc(BILL_ID) LOCAL INITRANS 20 PARALLEL;
ALTER INDEX IDX_BILL_LINE_CALC_BILL_ID NOPARALLEL;