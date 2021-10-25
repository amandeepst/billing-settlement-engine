package com.worldpay.pms.bse.domain.validation;

import com.worldpay.pms.bse.domain.model.BillableItem;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;

public interface ValidationService {

  Validation<Seq<DomainError>, BillableItem> apply(BillableItem billableItem);

}
