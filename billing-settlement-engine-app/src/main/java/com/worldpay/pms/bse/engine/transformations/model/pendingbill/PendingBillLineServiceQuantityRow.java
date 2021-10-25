package com.worldpay.pms.bse.engine.transformations.model.pendingbill;

import com.worldpay.pms.bse.domain.model.PendingBillLineServiceQuantity;
import com.worldpay.pms.bse.engine.transformations.model.input.pending.InputPendingBillLineServiceQuantityRow;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.aggregation.PendingBillLineServiceQuantityAggKey;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PendingBillLineServiceQuantityRow implements PendingBillLineServiceQuantity {

  private String serviceQuantityTypeCode;
  private BigDecimal serviceQuantityValue;
  private long partitionId;

  public static PendingBillLineServiceQuantityRow from(InputPendingBillLineServiceQuantityRow raw) {
    return new PendingBillLineServiceQuantityRow(
        raw.getServiceQuantityTypeCode(),
        raw.getServiceQuantityValue(),
        raw.getPartitionId()
    );
  }

  public PendingBillLineServiceQuantityAggKey aggregationKey() {
    return new PendingBillLineServiceQuantityAggKey(
        serviceQuantityTypeCode
    );
  }
}
