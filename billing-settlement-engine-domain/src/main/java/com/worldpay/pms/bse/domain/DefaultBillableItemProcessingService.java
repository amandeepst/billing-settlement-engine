package com.worldpay.pms.bse.domain;

import com.worldpay.pms.bse.domain.account.AccountDeterminationService;
import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountKey;
import com.worldpay.pms.bse.domain.model.BillableItem;
import com.worldpay.pms.bse.domain.validation.CalculationCheckService;
import com.worldpay.pms.bse.domain.validation.ValidationResult;
import com.worldpay.pms.bse.domain.validation.ValidationService;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import io.vavr.control.Validation;

public class DefaultBillableItemProcessingService implements BillableItemProcessingService {

  ValidationService validationService;
  CalculationCheckService calculationCheckService;
  AccountDeterminationService accountDeterminationService;

  public DefaultBillableItemProcessingService(ValidationService validationService, CalculationCheckService calculationCheckService,
      AccountDeterminationService accountDeterminationService) {

    this.accountDeterminationService = accountDeterminationService;
    this.validationService = validationService;
    this.calculationCheckService = calculationCheckService;
  }

  @Override
  public Validation<Seq<DomainError>, ValidationResult> process(BillableItem billableItem) {
    Validation<Seq<DomainError>, BillableItem> validation = validationService.apply(billableItem);

    AccountKey key = new AccountKey(billableItem.getSubAccountId(),
        billableItem.getAccruedDate().toLocalDate());

    return validation
        .flatMap(bi -> accountDeterminationService.findAccount(key).mapError(List::of))
        .map(account -> {
          if (!"FUND".equals(account.getAccountType()) && billableItem.isStandardBillableItem() &&
              !calculationCheckService.apply(billableItem)) {
            return new ValidationResult(account, false);
          }
          return new ValidationResult(account, true);
        });
  }
}
