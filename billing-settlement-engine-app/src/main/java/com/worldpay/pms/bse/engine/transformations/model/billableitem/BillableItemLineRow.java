package com.worldpay.pms.bse.engine.transformations.model.billableitem;

import com.worldpay.pms.bse.domain.model.BillableItemLine;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class BillableItemLineRow implements BillableItemLine {

  private String calculationLineClassification;

  private BigDecimal amount;
  private String calculationLineType;

  private String rateType;

  private BigDecimal rateValue;
  private int partitionId;
}
