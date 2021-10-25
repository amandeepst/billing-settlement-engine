package com.worldpay.pms.bse.domain.mincharge;

import com.worldpay.pms.bse.domain.model.mincharge.MinimumCharge;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumChargeKey;
import io.vavr.control.Option;

public interface MinimumChargeStore {

  Option<MinimumCharge> get(MinimumChargeKey key);



}
