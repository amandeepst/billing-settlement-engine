package com.worldpay.pms.bse.domain.tax;

import com.worldpay.pms.bse.domain.common.BillLineDomainError;
import com.worldpay.pms.bse.domain.model.billtax.BillTax;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBill;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;
import lombok.NonNull;

@FunctionalInterface
public interface TaxCalculationService {

  Validation<Seq<BillLineDomainError>, BillTax> calculate(@NonNull CompleteBill bill);
}
