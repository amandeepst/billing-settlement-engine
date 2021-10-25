package com.worldpay.pms.bse.engine.transformations.model.pendingbill;

import com.worldpay.pms.bse.domain.model.PendingBillLineCalculation;
import com.worldpay.pms.bse.engine.transformations.model.input.pending.InputPendingBillLineCalculationRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.aggregation.PendingBillLineCalculationAggKey;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class PendingBillLineCalculationRow implements PendingBillLineCalculation {

  private String billLineCalcId;
  private String calculationLineClassification;
  private String calculationLineType;
  private BigDecimal amount;
  private String includeOnBill;
  private String rateType;
  private BigDecimal rateValue;
  private long partitionId;

  public static PendingBillLineCalculationRow from(InputPendingBillLineCalculationRow raw) {
    return new PendingBillLineCalculationRow(
        raw.getBillLineCalcId(),
        raw.getCalculationLineClassification(),
        raw.getCalculationLineType(),
        raw.getAmount(),
        raw.getIncludeOnBill(),
        raw.getRateType(),
        raw.getRateValue(),
        raw.getPartitionId()
    );
  }

  public PendingBillLineCalculationAggKey aggregationKey() {
    return new PendingBillLineCalculationAggKey(
        calculationLineClassification,
        calculationLineType,
        includeOnBill,
        rateType
    );
  }
}
