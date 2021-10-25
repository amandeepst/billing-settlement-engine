package com.worldpay.pms.bse.domain;

import com.worldpay.pms.bse.domain.model.BillableItem;
import com.worldpay.pms.bse.domain.validation.ValidationResult;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;

public interface BillableItemProcessingService {

  Validation<Seq<DomainError>, ValidationResult> process(BillableItem billableItem);

}
