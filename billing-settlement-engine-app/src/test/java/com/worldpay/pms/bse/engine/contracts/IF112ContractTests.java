package com.worldpay.pms.bse.engine.contracts;

import static com.worldpay.pms.spark.core.Resource.resourceAsString;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class IF112ContractTests extends DbContractTest{

  @Test
  void canRunBillIdMap() {
    String query = resourceAsString("contracts/if112/if112_bill_id_map.sql")
        .replace("cm_bill_id_map@if112_oes_ormb_dblink", "vw_bill_id_map");

    String plan = runAndGetPlan("if112_bill_id_map", query, ImmutableMap.of(
        "c_ilm_dt", "2021-04-01 00:00:00",
        "c_start_time", "2021-04-05 00:00:00"
    ));

    log.info(plan);
  }
}