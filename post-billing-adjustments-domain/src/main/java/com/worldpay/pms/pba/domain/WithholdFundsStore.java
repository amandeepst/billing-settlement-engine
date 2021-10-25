package com.worldpay.pms.pba.domain;

import com.worldpay.pms.pba.domain.model.WithholdFunds;
import io.vavr.control.Option;

public interface WithholdFundsStore {
  Option<WithholdFunds> getWithholdFundForAccount(String accountId);
}
