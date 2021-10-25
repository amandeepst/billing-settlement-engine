select
    CASE WHEN blc1.amount = -blc2.amount THEN '1' ELSE '0' END AS correctly_negated
from bill_line_calc blc1,
     bill_line_calc blc2,
     bill_line bl1, bill_line bl2
where blc1.bill_ln_id = bl1.bill_ln_id
  and blc2.bill_ln_id = bl2.bill_ln_id
  and bl1.bill_ln_id = bl2.prev_bill_ln

