package com.worldpay.pms.bse.engine.domain.billcorrection;

import com.worldpay.pms.spark.core.factory.Factory;
import io.vavr.control.Try;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class BillCorrectionServiceFactory implements Factory<BillCorrectionService> {

  @Override
  public Try<BillCorrectionService> build() {
    return Try.of(BillCorrectionService::new);
  }
}
