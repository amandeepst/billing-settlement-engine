package com.worldpay.pms.bse.domain.model.completebill;

import com.worldpay.pms.bse.domain.common.Utils;
import com.worldpay.pms.bse.domain.model.PendingBillLineCalculation;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumCharge;
import java.math.BigDecimal;
import java.util.function.UnaryOperator;
import lombok.Builder;
import lombok.Value;
import lombok.With;

@With
@Builder
@Value
public class CompleteBillLineCalculation {

  String billLineCalcId;
  String calculationLineClassification;
  String calculationLineType;
  BigDecimal amount;
  String includeOnBill;
  String rateType;
  BigDecimal rateValue;

  public static CompleteBillLineCalculation from(PendingBillLineCalculation pendingBillLineCalculation,
      UnaryOperator<BigDecimal> rounder) {
    return new CompleteBillLineCalculation(
        pendingBillLineCalculation.getBillLineCalcId(),
        pendingBillLineCalculation.getCalculationLineClassification(),
        pendingBillLineCalculation.getCalculationLineType(),
        rounder.apply(pendingBillLineCalculation.getAmount()),
        pendingBillLineCalculation.getIncludeOnBill(),
        pendingBillLineCalculation.getRateType(),
        pendingBillLineCalculation.getRateValue()
    );
  }

  public static CompleteBillLineCalculation from(MinimumCharge minimumCharge, BigDecimal minChargeAmount,
      UnaryOperator<BigDecimal> rounder) {
    return new CompleteBillLineCalculation(
        Utils.generateId(),
        "BASE_CHG",
        "TOTAL_BB",
        rounder.apply(minChargeAmount),
        "Y",
        minimumCharge.getRateType(),
        minimumCharge.getRate()
    );
  }
}