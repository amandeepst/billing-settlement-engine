with STG_298_CM_INVOICE_DATA_LN as (
    select
    'NRML' as REC_TYPE,
    null as BILL_ID,
    null as BSEG_ID
    from dual
)
select /*+ ENABLE_PARALLEL_DML PARALLEL(CM_INV_DATA_LN_RATE) APPEND */
	CM_INV_DATA_LN_RATE.BILL_ID	C1_BILL_ID,
	CM_INV_DATA_LN_RATE.BSEG_ID	C2_BSEG_ID,
	CM_INV_DATA_LN_RATE.RATE_TP	C3_RATE_TP,
	CM_INV_DATA_LN_RATE.RATE	C4_RATE,
	CM_INV_DATA_LN_RATE.ILM_DT	C5_UPLOAD_DTTM,
	CM_INV_DATA_LN_RATE.EXTRACT_FLG	C6_EXTRACT_FLG,
	CM_INV_DATA_LN_RATE.EXTRACT_DTTM	C7_EXTRACT_DTTM
from	ODI_WORK.CM_INV_DATA_LN_RATE   CM_INV_DATA_LN_RATE, STG_298_CM_INVOICE_DATA_LN   STG_CM_INVOICE_DATA_LN
where	(1=1)
And (        CM_INV_DATA_LN_RATE.ILM_DT  >= TO_DATE( :startDate,'YYYY/MM/DD HH24:MI:SS')
AND CM_INV_DATA_LN_RATE.ILM_DT  < TO_DATE( :endDate,'YYYY/MM/DD HH24:MI:SS')
)
 And (STG_CM_INVOICE_DATA_LN.REC_TYPE = 'NRML')

 And (CM_INV_DATA_LN_RATE.BILL_ID=STG_CM_INVOICE_DATA_LN.BILL_ID AND CM_INV_DATA_LN_RATE.BSEG_ID=STG_CM_INVOICE_DATA_LN.BSEG_ID)
