package com.worldpay.pms.bse.engine.contracts;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.google.common.collect.ImmutableMap;
import java.sql.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class AccountingContractTest extends DbContractTest{

  @Test
  void canRunMAMQuery() {
    String query = resourceAsString("contracts/MAM_post_bill_adj_accounting.sql");

    String plan = runAndGetPlan("MAM_post_bill_adj", query, ImmutableMap.of(
        "low", Timestamp.valueOf("2021-02-10 00:00:00"),
        "high", Timestamp.valueOf("2021-02-11 00:00:00")
    ));

    log.info(plan);
  }
}
