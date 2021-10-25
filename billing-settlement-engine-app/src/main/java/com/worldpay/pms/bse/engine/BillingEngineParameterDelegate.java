package com.worldpay.pms.bse.engine;

import com.beust.jcommander.Parameter;
import com.worldpay.pms.bse.domain.exception.BillingException;
import com.worldpay.pms.cli.PmsParameterDelegate;
import com.worldpay.pms.cli.converter.LogicalDateConverter;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import lombok.Getter;

@Getter
public class BillingEngineParameterDelegate implements PmsParameterDelegate {

  @Parameter(
      names = {"--logical-date"}, arity = 1,
      description = "Date when the txns happened.",
      required = true,
      converter = LogicalDateConverter.class)
  private LocalDate logicalDate;

  @Parameter(
      names = {"--run-id"},
      arity = 1,
      description = "Force a particular run id rather than generating one",
      required = false)
  private Long runId;

  @Parameter(
      names = {"--processing-group"},
      variableArity = true,
      description = "Forces app to complete bills only for a specific group",
      required = true)
  private List<String> processingGroups;

  @Parameter(
      names = {"--override-event-date"},
      arity = 0,
      description = "Overrides the billable item accrued date using the value passed to --logical-date")
  private boolean overrideBillableItemAccruedDate = false;

  @Parameter(
      names = {"--billing-type"},
      variableArity = true,
      description = "Completes bills with adhoc flag `N` if `STANDARD`, with adhoc flag `Y` if `ADHOC`, both if `STANDARD` `ADHOC`")
  private List<String> billingTypes = Collections.singletonList("STANDARD");


  public static BillingEngineParameterDelegate getParameters(PmsParameterDelegate delegate) {
    if (delegate instanceof BillingEngineParameterDelegate) {
      return (BillingEngineParameterDelegate) delegate;
    } else {
      throw new BillingException("Parameter delegate corrupted. Impossible state.");
    }
  }
}