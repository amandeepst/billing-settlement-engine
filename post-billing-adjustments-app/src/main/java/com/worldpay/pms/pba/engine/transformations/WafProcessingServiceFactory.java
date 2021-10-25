package com.worldpay.pms.pba.engine.transformations;

import com.worldpay.pms.pba.engine.PostBillingConfiguration;
import com.worldpay.pms.pba.engine.PostBillingConfiguration.Store;
import com.worldpay.pms.pba.engine.data.PostBillingRepository;
import com.worldpay.pms.pba.domain.DefaultWafProcessingService;
import com.worldpay.pms.pba.domain.LazyWithholdFundsBalanceStore;
import com.worldpay.pms.pba.domain.PreloadedSubAccountStore;
import com.worldpay.pms.pba.domain.PreloadedWithholdFundsStore;
import com.worldpay.pms.pba.domain.SubAccountStore;
import com.worldpay.pms.pba.domain.WafProcessingService;
import com.worldpay.pms.pba.domain.WithholdFundsBalanceStore;
import com.worldpay.pms.pba.domain.WithholdFundsStore;
import com.worldpay.pms.pba.domain.model.WithholdFunds;
import com.worldpay.pms.pba.domain.model.WithholdFundsBalance;
import com.worldpay.pms.spark.core.factory.Factory;
import io.vavr.Tuple2;
import io.vavr.collection.Stream;
import io.vavr.control.Option;
import io.vavr.control.Try;

public class WafProcessingServiceFactory implements Factory<WafProcessingService> {

  private final Factory<PostBillingRepository> repositoryFactory;
  private final PostBillingConfiguration.Store configuration;

  public WafProcessingServiceFactory(
      Factory<PostBillingRepository> repositoryFactory,
      Store configuration) {
    this.repositoryFactory = repositoryFactory;
    this.configuration = configuration;
  }

  @Override
  public Try<WafProcessingService> build() {
    PostBillingRepository repository = repositoryFactory.build().get();
    return Try.of(() -> new DefaultWafProcessingService(
        createWithholdFundsStore(repository),
        createWithholdFundsBalanceStore(repository),
        createSubAccountStore(repository)
    ));
  }

  private WithholdFundsStore createWithholdFundsStore(PostBillingRepository source) {
    PreloadedWithholdFundsStore store = new PreloadedWithholdFundsStore(configuration.getMaxEntries(),
        configuration.getConcurrency(),
        configuration.getStatsInterval(),
        s -> Option.none());
    store.preload(Stream.ofAll(source.getWithholdFunds()).groupBy(WithholdFunds::getAccountId));
    return store;
  }

  private WithholdFundsBalanceStore createWithholdFundsBalanceStore(PostBillingRepository source) {
    return new LazyWithholdFundsBalanceStore(configuration.getMaxEntries(),
        configuration.getConcurrency(),
        configuration.getStatsInterval(),
        key -> Stream.ofAll(source.getWithholdFundsBalance(key.getPartyId(), key.getLegalCounterparty(), key.getCurrency()))
            .map(WithholdFundsBalance::getBalance)
            .toOption());
  }

  private SubAccountStore createSubAccountStore(PostBillingRepository source) {
    PreloadedSubAccountStore store = new PreloadedSubAccountStore(configuration.getMaxEntries(),
        configuration.getConcurrency(),
        configuration.getStatsInterval(),
        s -> Option.none());
    store.preload(Stream.ofAll(source.getWafSubAccountIds()).groupBy(Tuple2::_1).mapValues(tuples -> tuples.map(Tuple2::_2)));
    return store;
  }
}
