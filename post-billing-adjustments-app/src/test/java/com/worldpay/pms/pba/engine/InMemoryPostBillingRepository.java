package com.worldpay.pms.pba.engine;

import com.worldpay.pms.pba.engine.data.PostBillingRepository;
import com.worldpay.pms.pba.domain.model.WithholdFunds;
import com.worldpay.pms.pba.domain.model.WithholdFundsBalance;
import com.worldpay.pms.pba.domain.model.WithholdFundsBalance.WithholdFundsBalanceKey;
import io.vavr.Tuple2;
import io.vavr.collection.Stream;
import java.util.Collection;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
public class InMemoryPostBillingRepository implements PostBillingRepository {

  @Singular("addWithholdFund")
  Collection<WithholdFunds> getWithholdFunds;

  @Singular("addWafSubAccountId")
  Collection<Tuple2<String, String>> wafSubAccountIds;

  @Singular("addWafBalance")
  Collection<WithholdFundsBalance> wafBalances;

  @Override
  public Iterable<WithholdFunds> getWithholdFunds() {
    return getWithholdFunds;
  }

  @Override
  public Iterable<Tuple2<String, String>> getWafSubAccountIds() {
    return wafSubAccountIds;
  }

  @Override
  public Iterable<WithholdFundsBalance> getWithholdFundsBalance(String partyId, String legalCounterpartyId, String currency) {
    return Stream.ofAll(wafBalances).groupBy(balance ->
        new WithholdFundsBalanceKey(balance.getPartyId(), balance.getLegalCounterparty(), balance.getCurrency()))
        .getOrElse(new WithholdFundsBalanceKey(partyId, legalCounterpartyId, currency), Stream.empty());

  }
}
