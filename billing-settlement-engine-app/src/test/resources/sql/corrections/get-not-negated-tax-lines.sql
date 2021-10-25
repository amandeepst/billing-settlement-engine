select bt1.bill_tax_id
from
    bill b1,
    bill_tax bt1,
    bill_tax_detail btd1
where
  b1.prev_bill_id is null
  and b1.bill_id in (select bill_id from cm_inv_recalc_stg)
  and b1.bill_id in (select prev_bill_id from bill)
  and bt1.bill_id = b1.bill_id
  and bt1.bill_tax_id = btd1.bill_tax_id
  and not exists
    (select 1
     from
         bill_tax bt2,
         bill_tax_detail btd2,
         bill b2
     where
             b2.prev_bill_id is not null
       and b1.bill_id = b2.prev_bill_id
       and bt2.bill_id = b2.bill_id
       and bt2.bill_tax_id = btd2.bill_tax_id
       and btd1.tax_stat = btd2.tax_stat
    )
