grant insert, update, delete on cisadm.cm_bill_due_dt to cbe_bse_appuser;
grant insert, update, delete on cisadm.cm_bill_payment_dtl to cbe_bse_appuser;
grant insert, update, delete on cisadm.cm_bill_payment_dtl_snapshot to cbe_bse_appuser;

create or replace synonym cbe_bse_owner.ci_currency_cd for cisadm.ci_currency_cd;
create or replace synonym cbe_bse_appuser.ci_currency_cd for cisadm.ci_currency_cd;
create or replace synonym cbe_bse_owner.ci_priceitem_char for cisadm.ci_priceitem_char;
create or replace synonym cbe_bse_appuser.ci_priceitem_char for cisadm.ci_priceitem_char;
create or replace synonym cbe_bse_owner.vw_misc_bill_item for cbe_cue_owner.vw_misc_bill_item;
create or replace synonym cbe_bse_appuser.vw_misc_bill_item for cbe_cue_owner.vw_misc_bill_item;
create or replace synonym cbe_bse_owner.vw_misc_bill_item_ln for cbe_cue_owner.vw_misc_bill_item_ln;
create or replace synonym cbe_bse_appuser.vw_misc_bill_item_ln for cbe_cue_owner.vw_misc_bill_item_ln;
create or replace synonym cbe_bse_owner.ci_priceasgn for cisadm.ci_priceasgn;
create or replace synonym cbe_bse_appuser.ci_priceasgn for cisadm.ci_priceasgn;
create or replace synonym cbe_bse_owner.ci_party for cisadm.ci_party;
create or replace synonym cbe_bse_appuser.ci_party for cisadm.ci_party;
create or replace synonym cbe_bse_owner.ci_per_id for cisadm.ci_per_id;
create or replace synonym cbe_bse_appuser.ci_per_id for cisadm.ci_per_id;
create or replace synonym cbe_bse_owner.ci_pricecomp for cisadm.ci_pricecomp;
create or replace synonym cbe_bse_appuser.ci_pricecomp for cisadm.ci_pricecomp;
create or replace synonym cbe_bse_owner.ci_per for cisadm.ci_per;
create or replace synonym cbe_bse_appuser.ci_per for cisadm.ci_per;
create or replace synonym cbe_bse_owner.ci_per_char for cisadm.ci_per_char;
create or replace synonym cbe_bse_appuser.ci_per_char for cisadm.ci_per_char;
create or replace synonym cbe_bse_owner.cm_bill_payment_dtl for cisadm.cm_bill_payment_dtl;
create or replace synonym cbe_bse_appuser.cm_bill_payment_dtl for cisadm.cm_bill_payment_dtl;
create or replace synonym cbe_bse_owner.cm_bill_payment_dtl_snapshot for cisadm.cm_bill_payment_dtl_snapshot;
create or replace synonym cbe_bse_appuser.cm_bill_payment_dtl_snapshot for cisadm.cm_bill_payment_dtl_snapshot;
create or replace synonym cbe_bse_owner.cm_bill_due_dt for cisadm.cm_bill_due_dt;
create or replace synonym cbe_bse_appuser.cm_bill_due_dt for cisadm.cm_bill_due_dt;
create or replace synonym cbe_bse_owner.cm_inv_recalc_stg for cisadm.cm_inv_recalc_stg;
create or replace synonym cbe_bse_appuser.cm_inv_recalc_stg for cisadm.cm_inv_recalc_stg;
create or replace synonym cbe_bse_owner.pay_dtl_id_sq for cisadm.pay_dtl_id_sq;
create or replace synonym cbe_bse_appuser.pay_dtl_id_sq for cisadm.pay_dtl_id_sq;
create or replace synonym cbe_bse_owner.bill_balance_id_seq for cisadm.bill_balance_id_seq;
create or replace synonym cbe_bse_appuser.bill_balance_id_seq for cisadm.bill_balance_id_seq;

GRANT SELECT ON CBE_PCE_OWNER.SOURCE_KEY_SQ TO CBE_BSE_OWNER;
GRANT SELECT ON CBE_PCE_OWNER.SOURCE_KEY_SQ TO CBE_BSE_APPUSER;

create or replace synonym CBE_BSE_OWNER.SOURCE_KEY_SQ for CBE_PCE_OWNER.SOURCE_KEY_SQ;
create or replace synonym CBE_BSE_APPUSER.SOURCE_KEY_SQ for CBE_PCE_OWNER.SOURCE_KEY_SQ;

create or replace synonym CBE_BSE_OWNER.CM_PAY_REQ_OVERPAY for CISADM.CM_PAY_REQ_OVERPAY;
create or replace synonym CBE_BSE_APPUSER.CM_PAY_REQ_OVERPAY for CISADM.CM_PAY_REQ_OVERPAY;
