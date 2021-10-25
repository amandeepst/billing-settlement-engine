package com.worldpay.pms.bse.engine.utils.junit;

import com.esotericsoftware.kryo.Kryo;
import com.worldpay.pms.bse.engine.BillingKryoRegistrator;
import com.worldpay.pms.bse.engine.DummyFactory;
import com.worldpay.pms.bse.engine.InMemoryAccountRepository;
import com.worldpay.pms.bse.engine.InMemoryBillIdGenerator;
import com.worldpay.pms.bse.engine.InMemoryBillingRepository;
import com.worldpay.pms.bse.engine.utils.TestValidationService;

public class TestingKryoRegistrator extends BillingKryoRegistrator {

  @Override
  public void registerClasses(Kryo kryo) {
    super.registerClasses(kryo);

    kryo.register(TestValidationService.class);
    kryo.register(DummyFactory.class);
    kryo.register(InMemoryAccountRepository.class);
    kryo.register(InMemoryBillingRepository.class);
    kryo.register(InMemoryBillIdGenerator.class);

    kryo.register(io.vavr.collection.Seq.class);
    kryo.register(io.vavr.collection.List.class);
    kryo.register(io.vavr.collection.List.Nil.class);
    kryo.register(io.vavr.collection.List.Cons.class);
    kryo.register(java.util.List.class);
    kryo.register(java.util.Collections.emptyList().getClass());
    kryo.register(java.util.Collections.singletonList("").getClass());
    kryo.register(java.util.concurrent.atomic.AtomicLong.class);

  }
}
