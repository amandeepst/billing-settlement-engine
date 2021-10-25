with STG_298_CM_INVOICE_DATA as (
    select
    'NRML' as REC_TYPE,
    null as BILL_ID
    from dual
)
select /*+ ENABLE_PARALLEL_DML PARALLEL(CM_INV_DATA_ADJ) APPEND */
	CM_INV_DATA_ADJ.BILL_ID	C1_BILL_ID,
	CM_INV_DATA_ADJ.ADJ_ID	C2_ADJ_ID,
	CM_INV_DATA_ADJ.ADJ_AMT	C3_ADJ_AMT,
	CM_INV_DATA_ADJ.ADJ_TYPE_CD	C4_ADJ_TYPE_CD,
	CM_INV_DATA_ADJ.CURRENCY_CD	C5_CURRENCY_CD,
	CM_INV_DATA_ADJ.DESCR	C6_DESCR,
	CM_INV_DATA_ADJ.ILM_DT	C7_UPLOAD_DTTM,
	CM_INV_DATA_ADJ.EXTRACT_FLG	C8_EXTRACT_FLG,
	CM_INV_DATA_ADJ.EXTRACT_DTTM	C9_EXTRACT_DTTM
from	ODI_WORK.CM_INV_DATA_ADJ   CM_INV_DATA_ADJ, ODI_WORK.STG_298_CM_INVOICE_DATA   STG_CM_INVOICE_DATA
where	(1=1)
And (        CM_INV_DATA_ADJ.EXTRACT_FLG='Y'
AND CM_INV_DATA_ADJ.ILM_DT  >= TO_DATE( :startDate,'YYYY/MM/DD HH24:MI:SS')
AND CM_INV_DATA_ADJ.ILM_DT  < TO_DATE( :endDate,'YYYY/MM/DD HH24:MI:SS') )
 And (STG_CM_INVOICE_DATA.REC_TYPE = 'NRML')

 And (CM_INV_DATA_ADJ.BILL_ID=STG_CM_INVOICE_DATA.BILL_ID)