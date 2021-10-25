package com.worldpay.pms.bse.engine.utils;

import com.worldpay.pms.bse.domain.model.BillableItem;
import com.worldpay.pms.bse.domain.validation.ValidationService;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;

public class TestValidationService implements ValidationService {

  private final boolean valid;

  public TestValidationService(boolean valid) {
    this.valid = valid;
  }

  @Override
  public Validation<Seq<DomainError>, BillableItem> apply(BillableItem billableItem) {
    if (valid) {
      return Validation.valid(billableItem);
    } else {
      return Validation.invalid(io.vavr.collection.List.of(new DomainError("test_code", "test_message")));
    }
  }
}