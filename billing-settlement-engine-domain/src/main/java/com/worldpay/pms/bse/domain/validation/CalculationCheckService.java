package com.worldpay.pms.bse.domain.validation;

import com.worldpay.pms.bse.domain.model.BillableItem;

public interface CalculationCheckService {


  Boolean apply(BillableItem billableItem);

}
