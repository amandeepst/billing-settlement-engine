package com.worldpay.pms.bse.engine.transformations.model.completebill;

import com.worldpay.pms.bse.domain.model.BillLineServiceQuantity;
import com.worldpay.pms.bse.domain.model.completebill.CompleteBillLineServiceQuantity;
import com.worldpay.pms.bse.engine.transformations.model.input.correction.InputBillLineSvcQtyCorrectionRow;
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
public class BillLineServiceQuantityRow implements BillLineServiceQuantity {

  @NonNull
  private String serviceQuantityCode;
  @NonNull
  private BigDecimal serviceQuantity;
  private String serviceQuantityDescription;

  public static BillLineServiceQuantityRow from(CompleteBillLineServiceQuantity completeBillLineServiceQuantity, String svcQtyDescription) {
    return new BillLineServiceQuantityRow(
        completeBillLineServiceQuantity.getServiceQuantityTypeCode(),
        completeBillLineServiceQuantity.getServiceQuantityValue(),
        svcQtyDescription
    );
  }

  public static BillLineServiceQuantityRow from(InputBillLineSvcQtyCorrectionRow billLineSvcQtyCorrection) {
    return new BillLineServiceQuantityRow(
        billLineSvcQtyCorrection.getServiceQuantityCode(),
        billLineSvcQtyCorrection.getServiceQuantity(),
        billLineSvcQtyCorrection.getServiceQuantityDescription()
    );
  }
}
