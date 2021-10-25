package com.worldpay.pms.bse.domain.model.completebill;

import com.worldpay.pms.bse.domain.model.PendingBillLineServiceQuantity;
import io.vavr.collection.List;
import java.math.BigDecimal;
import java.util.function.UnaryOperator;
import lombok.Builder;
import lombok.Value;
import lombok.With;

@With
@Builder
@Value
public class CompleteBillLineServiceQuantity {

  static final List<String> ROUNDED_TYPES = List.of("PI_AMT", "PC_AMT", "IC_AMT", "SF_AMT", "TOTAL_AMT");

  String serviceQuantityTypeCode;
  BigDecimal serviceQuantityValue;

  public static CompleteBillLineServiceQuantity from(PendingBillLineServiceQuantity pendingBillLineServiceQuantity,
      UnaryOperator<BigDecimal> roundingFunction) {
    String type = pendingBillLineServiceQuantity.getServiceQuantityTypeCode();
    BigDecimal value = ROUNDED_TYPES.contains(type.toUpperCase()) ?
        roundingFunction.apply(pendingBillLineServiceQuantity.getServiceQuantityValue())
        : pendingBillLineServiceQuantity.getServiceQuantityValue();

    return new CompleteBillLineServiceQuantity(type, value);
  }
}
