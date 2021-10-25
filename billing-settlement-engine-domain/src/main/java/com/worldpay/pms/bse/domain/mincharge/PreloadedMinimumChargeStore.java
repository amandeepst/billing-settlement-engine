package com.worldpay.pms.bse.domain.mincharge;

import com.worldpay.pms.bse.domain.common.LazyStore;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumCharge;
import com.worldpay.pms.bse.domain.model.mincharge.MinimumChargeKey;
import io.vavr.Function1;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import io.vavr.control.Try;
import lombok.Builder;
import lombok.NonNull;

public class PreloadedMinimumChargeStore extends LazyStore<MinimumChargeKey, Option<MinimumCharge>>
    implements MinimumChargeStore {

  @Builder
  public PreloadedMinimumChargeStore(@NonNull Integer maximumSize, @NonNull Integer concurrencyLevel,
      @NonNull Integer printStatsInterval,
      @NonNull Function1<MinimumChargeKey, Option<MinimumCharge>> loader) {
    super(maximumSize, concurrencyLevel, printStatsInterval, loader);

  }

  public void preloadMinCharge(Iterable<MinimumCharge> minimumCharges) {
    super.preload(
        Stream.ofAll(minimumCharges)
            .groupBy(MinimumChargeKey::from)
            .mapValues(minCharge -> minCharge.minBy(MinimumCharge::getRate))
            .mapValues(Try::success)
            .toJavaMap()
    );
  }
}
