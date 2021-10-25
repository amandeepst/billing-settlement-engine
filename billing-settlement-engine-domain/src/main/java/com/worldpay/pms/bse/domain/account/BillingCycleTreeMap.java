package com.worldpay.pms.bse.domain.account;

import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.control.Option;
import java.time.LocalDate;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class BillingCycleTreeMap {

  private Map<String, TreeMap<LocalDate, BillingCycle>> billingCycleMap;

  public BillingCycleTreeMap(Iterable<BillingCycle> billingCycles) {
    billingCycleMap = List.ofAll(billingCycles).groupBy(BillingCycle::getCode)
        .mapValues(cycles -> cycles.map(
            billingCycle -> new Tuple2<LocalDate, BillingCycle>(billingCycle.getEndDate(),
                billingCycle))
            .collect(io.vavr.collection.TreeMap.collector())
            .toJavaMap())
        .toJavaMap();
  }

  public Option<BillingCycle> getBillingCycleThatEndsAfterDate(String code, LocalDate date) {
    return getBillingCycleByCodeAndEndDate(code, date);
  }

  public Option<BillingCycle> getBillingCycleThatContainsDate(String code, LocalDate date) {
    Option<BillingCycle> billingCycleOp = getBillingCycleByCodeAndEndDate(code, date);
    return billingCycleOp.filter(bc -> bc.getStartDate().isBefore(date) || bc.getStartDate().isEqual(date));
  }

  private Option<BillingCycle> getBillingCycleByCodeAndEndDate(String code, LocalDate date) {
    TreeMap<LocalDate, BillingCycle> treeMapForCode = billingCycleMap.get(code);
    if (treeMapForCode == null) {
      return Option.none();
    }
    Entry<LocalDate, BillingCycle> entry = treeMapForCode.ceilingEntry(date);
    if (entry == null) {
      return Option.none();
    }
    return Option.of(entry.getValue());
  }

}
