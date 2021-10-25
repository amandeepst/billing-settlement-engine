package com.worldpay.pms.bse.domain.account;

import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.List;
import io.vavr.control.Validation;
import java.time.LocalDate;
import lombok.Builder;
import lombok.NonNull;

public class DefaultAccountDeterminationService implements AccountDeterminationService {

  private AccountDataStore accountDataStore;
  private LazyAccountDetailsStore accountDetailsStore;
  private BillingCycleTreeMap billingCycleTreeMap;
  private AccountRepository repository;

  @Builder
  public DefaultAccountDeterminationService(
      @NonNull Integer maximumSize, @NonNull Integer concurrencyLevel,
      @NonNull Integer printStatsInterval, AccountRepository repository ) {
    this.billingCycleTreeMap = new BillingCycleTreeMap(repository.getBillingCycles());
    this.accountDetailsStore = new LazyAccountDetailsStore(
        maximumSize, concurrencyLevel, printStatsInterval,
        acctId -> AccountDeterminationService.validateAccountDetails(acctId, repository.getAccountDetailsByAccountId(acctId)));
    this.accountDataStore = new LazyAccountDataStore(maximumSize, concurrencyLevel, printStatsInterval,
        key -> {
          Validation<DomainError, BillingAccount> account =
            AccountDeterminationService.validateAccounts(key.toString(), repository.getAccountBySubAccountId(key));
          if(account.isInvalid()){
            return account;
          }
          BillingAccount ac = account.get();
          accountDetailsStore.preload(
              List.of(new AccountDetails(ac.getAccountId(), ac.getProcessingGroup(), ac.getCode())));
          Validation<DomainError, BillingCycle> billingCycle =
              getBillingCycleThatContainsDate(account.get().getCode(), key.getAccruedDate());
          if(billingCycle.isInvalid()){
            return billingCycle.getError().asValidation();
          }
          return account.map( a -> {a.setBillingCycle(billingCycle.get()); return a;});
        });
    this.repository = repository;
  }

  @Override
  public Validation<DomainError, BillingAccount> findAccount(@NonNull AccountKey accountKey) {
    return accountDataStore.get(accountKey);
  }

  @Override
  public Validation<DomainError, BillingCycle> getBillingCycleForAccountId(@NonNull String accountId,
      @NonNull LocalDate date) {
    return accountDetailsStore.get(accountId)
        .flatMap(details -> AccountDeterminationService
            .validateBillingCycle(details.getBillingCycleCode(), date,
                billingCycleTreeMap
                    .getBillingCycleThatEndsAfterDate(details.getBillingCycleCode(), date)));
  }

  @Override
  public Validation<DomainError, String> getProcessingGroupForAccountId(@NonNull String accountId) {
    return accountDetailsStore.get(accountId).map(AccountDetails::getProcessingGroup);
  }

  @Override
  public Validation<DomainError, String> getBillingCycleCodeForAccountId(@NonNull String accountId) {
    return accountDetailsStore.get(accountId).map(AccountDetails::getBillingCycleCode);
  }

  @Override
  public Validation<DomainError, BillingAccount> getChargingAccountByPartyId(String partyId, String legalCounterParty, String currency) {
    return AccountDeterminationService.validateAccounts(String.format("partyId=%s, legalCounterpartyId=%s, currency=%s, accountType=CHRG",
        partyId, legalCounterParty, currency),
        repository.getChargingAccountByPartyId(partyId, legalCounterParty, currency));
  }

  private Validation<DomainError, BillingCycle> getBillingCycleThatContainsDate(String code, LocalDate date){
    return AccountDeterminationService
        .validateBillingCycle(code, date, billingCycleTreeMap.getBillingCycleThatContainsDate(code, date));
  }
}
