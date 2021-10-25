package com.worldpay.pms.bse.domain.validation;

import com.worldpay.pms.bse.domain.model.BillableItemLine;
import io.vavr.collection.List;
import java.math.BigDecimal;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TestBillableItemLine implements BillableItemLine {

  private String calculationLineClassification;

  private BigDecimal amount;
  private String calculationLineType;

  private String rateType;

  private BigDecimal rateValue;
  private int partitionId;


  public static TestBillableItemLine[] getBillableItemLines() {
    return List.of(TestBillableItemLine.builder()
            .calculationLineClassification("BASE_CHG")
            .amount(new BigDecimal("1.05"))
            .calculationLineType("PC_MBA")
            .rateType("MSC_PC")
            .rateValue(new BigDecimal("0.01"))
            .partitionId(1)
            .build(),
        TestBillableItemLine.builder()
            .calculationLineClassification("BASE_CHG")
            .amount(new BigDecimal("2.04"))
            .calculationLineType("PI_MBA")
            .rateType("MSC_PI")
            .rateValue(new BigDecimal("0.2"))
            .partitionId(1)
            .build()
    ).toJavaArray(TestBillableItemLine[]::new);
  }

  public static TestBillableItemLine[] getBigAmountBillableItemLines() {
    return List.of(TestBillableItemLine.builder()
            .calculationLineClassification("BASE_CHG")
            .amount(new BigDecimal("1562.05124"))
            .calculationLineType("PC_MBA")
            .rateType("MSC_PC")
            .rateValue(new BigDecimal("0.00165"))
            .partitionId(1)
            .build(),
        TestBillableItemLine.builder()
            .calculationLineClassification("BASE_CHG")
            .amount(new BigDecimal("200.04"))
            .calculationLineType("PI_MBA")
            .rateType("MSC_PI")
            .rateValue(new BigDecimal("0.02"))
            .partitionId(1)
            .build()
    ).toJavaArray(TestBillableItemLine[]::new);
  }

  public static TestBillableItemLine getBillableItemLineWithNullCalcLnType() {
    return TestBillableItemLine.builder()
        .calculationLineClassification("BASE_CHG")
        .amount(new BigDecimal("1.05"))
        .rateType("MSC_PC")
        .rateValue(new BigDecimal("0.01"))
        .partitionId(1)
        .build();
  }
}
