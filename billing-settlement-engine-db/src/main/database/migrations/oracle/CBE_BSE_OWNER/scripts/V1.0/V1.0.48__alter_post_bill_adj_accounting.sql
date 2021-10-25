ALTER TABLE post_bill_adj_accounting ADD (
partition           NUMBER(3,0)   GENERATED ALWAYS AS (MOD(partition_id, 64)) VIRTUAL
)