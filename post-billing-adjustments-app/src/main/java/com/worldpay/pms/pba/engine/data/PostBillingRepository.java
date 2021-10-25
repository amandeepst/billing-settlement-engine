package com.worldpay.pms.pba.engine.data;

import com.worldpay.pms.pba.domain.model.WithholdFunds;
import com.worldpay.pms.pba.domain.model.WithholdFundsBalance;
import io.vavr.Tuple2;

public interface PostBillingRepository {

  Iterable<WithholdFunds> getWithholdFunds();

  Iterable<Tuple2<String, String>> getWafSubAccountIds();

  Iterable<WithholdFundsBalance> getWithholdFundsBalance(String partyId, String legalCounterpartyId, String currency);
}
