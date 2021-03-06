with STG_298_CM_INVOICE_DATA_LN as (
    select
    'NRML' as REC_TYPE,
    null as BILL_ID,
    null as BSEG_ID
    from dual
)
select /*+ ENABLE_PARALLEL_DML PARALLEL(CM_INV_DATA_LN_BCL) */
	CM_INV_DATA_LN_BCL.BILL_ID	C1_BILL_ID,
	CM_INV_DATA_LN_BCL.BSEG_ID	C2_BSEG_ID,
	CM_INV_DATA_LN_BCL.BCL_TYPE	C3_BCL_TYPE,
	CM_INV_DATA_LN_BCL.BCL_DESCR	C4_BCL_DESCR,
	CM_INV_DATA_LN_BCL.CALC_AMT	C5_CALC_AMT,
	CM_INV_DATA_LN_BCL.TAX_STAT	C6_TAX_STAT,
	CM_INV_DATA_LN_BCL.TAX_STAT_DESCR	C7_TAX_STAT_DESCR,
	CM_INV_DATA_LN_BCL.TAX_RATE	C8_TAX_RATE,
	CM_INV_DATA_LN_BCL.ILM_DT	C9_UPLOAD_DTTM,
	CM_INV_DATA_LN_BCL.EXTRACT_FLG	C10_EXTRACT_FLG,
	CM_INV_DATA_LN_BCL.EXTRACT_DTTM	C11_EXTRACT_DTTM
from	ODI_WORK.CM_INV_DATA_LN_BCL   CM_INV_DATA_LN_BCL, STG_298_CM_INVOICE_DATA_LN   STG_CM_INVOICE_DATA_LN
where	(1=1)
And (        CM_INV_DATA_LN_BCL.ILM_DT  >= TO_DATE( :startDate,'YYYY/MM/DD HH24:MI:SS')
AND CM_INV_DATA_LN_BCL.ILM_DT  < TO_DATE( :endDate,'YYYY/MM/DD HH24:MI:SS') )
 And (STG_CM_INVOICE_DATA_LN.REC_TYPE = 'NRML')

 And (        CM_INV_DATA_LN_BCL.BILL_ID = STG_CM_INVOICE_DATA_LN.BILL_ID
AND CM_INV_DATA_LN_BCL.BSEG_ID = STG_CM_INVOICE_DATA_LN.BSEG_ID)
