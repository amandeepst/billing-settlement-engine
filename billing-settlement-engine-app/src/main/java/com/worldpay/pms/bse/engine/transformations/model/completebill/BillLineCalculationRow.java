package com.worldpay.pms.bse.engine.transformations.model.completebill;

import com.worldpay.pms.bse.domain.common.Utils;
import com.worldpay.pms.bse.domain.model.BillLineCalculation;
import com.worldpay.pms.bse.domain.model.billtax.BillLineTax;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLineCalculation;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillLineCalcCorrectionRow;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class BillLineCalculationRow implements BillLineCalculation {

  @NonNull
  private String billLineCalcId;
  @NonNull
  private String calculationLineClassification;
  private String calculationLineType;
  private String calculationLineTypeDescription;
  @NonNull
  private BigDecimal amount;
  @NonNull
  private String includeOnBill;
  private String rateType;
  private BigDecimal rateValue;
  private String taxStatus;
  private BigDecimal taxRate;
  private String taxStatusDescription;


  public static BillLineCalculationRow from(CompleteBillLineCalculation completeBillLineCalculation, String calcLineTypeDescription,
      BillLineTax billLineTax) {
    return new BillLineCalculationRow(
        Utils.getOrDefault(completeBillLineCalculation.getBillLineCalcId(), Utils::generateId),
        completeBillLineCalculation.getCalculationLineClassification(),
        completeBillLineCalculation.getCalculationLineType(),
        calcLineTypeDescription,
        completeBillLineCalculation.getAmount(),
        completeBillLineCalculation.getIncludeOnBill(),
        completeBillLineCalculation.getRateType(),
        completeBillLineCalculation.getRateValue(),
        billLineTax == null ? null : billLineTax.getTaxStatus(),
        billLineTax == null ? null : billLineTax.getTaxRate(),
        billLineTax == null ? null : billLineTax.getTaxStatusDescription()
    );
  }

  public static BillLineCalculationRow from(InputBillLineCalcCorrectionRow lineCalcCorrection) {
    return new BillLineCalculationRow(
        lineCalcCorrection.getBillLineCalcId(),
        lineCalcCorrection.getCalculationLineClassification(),
        lineCalcCorrection.getCalculationLineType(),
        lineCalcCorrection.getCalculationLineTypeDescription(),
        lineCalcCorrection.getAmount(),
        lineCalcCorrection.getIncludeOnBill(),
        lineCalcCorrection.getRateType(),
        lineCalcCorrection.getRateValue(),
        lineCalcCorrection.getTaxStatus(),
        lineCalcCorrection.getTaxRate(),
        lineCalcCorrection.getTaxStatusDescription()
    );
  }
}
