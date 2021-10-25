ALTER SEQUENCE non_event_id_sq INCREMENT BY 1;
alter table bill_price enable row movement;
alter table post_bill_adj enable row movement;

update post_bill_adj
set non_event_id = non_event_id_sq.nextval,
ilm_dt = sysdate
where non_event_id is null;

update bill_price
set non_event_id = non_event_id_sq.nextval,
ilm_dt = sysdate
where non_event_id is null;

update bill_price
set ilm_dt = sysdate
where source_type = 'REC_CHG';

commit;

ALTER SEQUENCE non_event_id_sq INCREMENT BY 10000;
alter table bill_price disable row movement;
alter table post_bill_adj disable row movement;