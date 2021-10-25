select
    case when
                     btd1.net_amt = -btd2.net_amt
                 and
                     btd1.tax_amt = -btd2.tax_amt
             THEN '1' ELSE '0' END  as corect_net_amount
from
    bill_tax bt1,
    bill_tax bt2,
    bill_tax_detail btd1,
    bill_tax_detail btd2,
    bill b1, bill b2
where
        b1.bill_id = b2.prev_bill_id
  and bt1.bill_id = b1.bill_id
  and bt1.bill_tax_id = btd1.bill_tax_id
  and bt2.bill_id = b2.bill_id
  and bt2.bill_tax_id = btd2.bill_tax_id
  and btd1.tax_stat = btd2.tax_stat


