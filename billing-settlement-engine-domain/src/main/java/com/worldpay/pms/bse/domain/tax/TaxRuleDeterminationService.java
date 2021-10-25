package com.worldpay.pms.bse.domain.tax;

import com.worldpay.pms.bse.domain.model.completebill.CompleteBill;
import com.worldpay.pms.bse.domain.model.tax.TaxRule;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.control.Validation;
import lombok.NonNull;

@FunctionalInterface
public interface TaxRuleDeterminationService {

  Validation<DomainError, TaxRule> apply(@NonNull CompleteBill bill);
}