package com.worldpay.pms.bse.engine.runresult;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@JsonIgnoreProperties(value = {"successCount"}, allowGetters = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ResultCount {

  long ignoredCount;
  long errorCount;
  long totalCount;
  long failedTodayCount;
  long fixedCount;

  public long getSuccessCount() {
    return getTotalCount() - getErrorCount();
  }
}
