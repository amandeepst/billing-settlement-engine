package com.worldpay.pms.bse.domain.tax;

import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLine;
import com.worldpay.pms.bse.domain.model.tax.TaxRate;
import com.worldpay.pms.bse.domain.model.tax.TaxRule;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.control.Validation;
import lombok.NonNull;

@FunctionalInterface
public interface TaxRateDeterminationService {

  Validation<DomainError, TaxRate> apply(@NonNull CompleteBillLine billLine, @NonNull TaxRule taxRule);
}
