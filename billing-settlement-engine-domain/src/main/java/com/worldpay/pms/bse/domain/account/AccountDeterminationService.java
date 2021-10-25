package com.worldpay.pms.bse.domain.account;

import com.worldpay.pms.bse.domain.common.ErrorCatalog;
import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import io.vavr.control.Validation;
import java.time.LocalDate;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

public interface AccountDeterminationService {

  Validation<DomainError, BillingAccount> findAccount(AccountKey accountKey);

  Validation<DomainError, BillingCycle> getBillingCycleForAccountId(String accountId, LocalDate date);

  Validation<DomainError, String> getProcessingGroupForAccountId(String accountId);

  Validation<DomainError, String> getBillingCycleCodeForAccountId(String accountId);

  Validation<DomainError, BillingAccount> getChargingAccountByPartyId(String partyId, String legalCounterParty, String currency);

  static Validation<DomainError, BillingAccount> validateAccounts(String identifier,
      Iterable<BillingAccount> accounts) {
    if (accounts == null || !accounts.iterator().hasNext()) {
      return ErrorCatalog.accountNotFound(identifier).asValidation();
    }
    return validateAccounts(identifier, Stream.ofAll(accounts));
  }

  static Validation<DomainError, BillingAccount> validateAccounts(String identifier,
      Stream<BillingAccount> accounts) {
    if (accounts.length() > 1) {
      return ErrorCatalog.multipleAccountsFound(identifier).asValidation();
    }
    BillingAccount account = accounts.single();
    return Validation.valid(account);
  }

  static Validation<DomainError, AccountDetails> validateAccountDetails(String accountId,
      Iterable<AccountDetails> accountDetailsByAccountId) {
    if (accountDetailsByAccountId == null || !accountDetailsByAccountId.iterator().hasNext()) {
      return ErrorCatalog.accountDetailsNotFound(accountId).asValidation();
    }
    Stream<AccountDetails> accountDetailsStream = Stream.ofAll(accountDetailsByAccountId);
    if (accountDetailsStream.length() > 1) {
      return ErrorCatalog.multipleAccountDetailsEntriesFound(accountId).asValidation();
    }
    return Validation.valid(accountDetailsStream.single());
  }

  static Validation<DomainError, BillingCycle> validateBillingCycle(
      String billCycleCode, LocalDate date,
      Option<BillingCycle> billingCycle) {
    if(billingCycle.isEmpty()){
      return ErrorCatalog.noBillingCycleFound(billCycleCode, date).asValidation();
    }
    return Validation.valid(billingCycle.get());
  }

  @Value
  class AccountKey {
    String subAccountId;
    LocalDate accruedDate;
  }

  @Value
  @AllArgsConstructor
  @Builder(toBuilder = true)
  class AccountDetails {
    String accountId;
    String processingGroup;
    String billingCycleCode;
  }

}
