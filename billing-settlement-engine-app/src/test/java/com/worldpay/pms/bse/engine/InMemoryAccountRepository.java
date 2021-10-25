package com.worldpay.pms.bse.engine;

import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountDetails;
import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountKey;
import com.worldpay.pms.bse.domain.account.AccountRepository;
import com.worldpay.pms.bse.domain.account.BillingAccount;
import com.worldpay.pms.bse.domain.account.BillingCycle;
import com.worldpay.pms.bse.domain.model.tax.Party;
import io.vavr.collection.Stream;
import java.util.Collection;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
public class InMemoryAccountRepository implements AccountRepository {

  @Singular("addAccount")
  Collection<BillingAccount> accounts;

  @Singular("addBillingCycle")
  Collection<BillingCycle> billingCycles;

  @Singular("addAccountDetail")
  Collection<AccountDetails> accountDetails;


  @Override
  public Iterable<BillingAccount> getAccountBySubAccountId(AccountKey key) {
    return Stream
        .ofAll(accounts)
        .filter(x -> x.getSubAccountId().equals(key.getSubAccountId()));
  }

  @Override
  public Iterable<BillingCycle> getBillingCycles() {
    return billingCycles;
  }

  @Override
  public Iterable<AccountDetails> getAccountDetailsByAccountId(String accountId) {
    return Stream
        .ofAll(accountDetails)
        .filter(x -> x.getAccountId().equals(accountId));
  }

  @Override
  public Iterable<Party> getParties() {
    return null;
  }

  @Override
  public Iterable<BillingAccount> getChargingAccountByPartyId(String partyId, String legalCounterParty, String currency) {
    return null;
  }
}
