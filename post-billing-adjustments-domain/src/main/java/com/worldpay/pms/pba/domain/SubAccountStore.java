package com.worldpay.pms.pba.domain;

import io.vavr.control.Option;

public interface SubAccountStore {

  Option<String> getWafSubAccountId(String accountId);
}
