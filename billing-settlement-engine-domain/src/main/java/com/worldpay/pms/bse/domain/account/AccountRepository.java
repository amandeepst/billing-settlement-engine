package com.worldpay.pms.bse.domain.account;

import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountDetails;
import com.worldpay.pms.bse.domain.account.AccountDeterminationService.AccountKey;
import com.worldpay.pms.bse.domain.model.tax.Party;

public interface AccountRepository {

  Iterable<BillingAccount> getAccountBySubAccountId(AccountKey key);

  Iterable <BillingCycle> getBillingCycles();

  Iterable<AccountDetails> getAccountDetailsByAccountId(String accountId);

  Iterable<Party> getParties();

  Iterable<BillingAccount> getChargingAccountByPartyId(String partyId, String legalCounterParty, String currency);
}
