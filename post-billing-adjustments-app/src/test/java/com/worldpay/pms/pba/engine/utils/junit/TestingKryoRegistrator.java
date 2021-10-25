package com.worldpay.pms.pba.engine.utils.junit;

import com.esotericsoftware.kryo.Kryo;
import com.worldpay.pms.pba.engine.DummyFactory;
import com.worldpay.pms.pba.engine.InMemoryPostBillingRepository;
import com.worldpay.pms.pba.engine.PostBillingKryoRegistrator;
import com.worldpay.pms.pba.engine.transformations.writers.TestData;

public class TestingKryoRegistrator extends PostBillingKryoRegistrator {

  @Override
  public void registerClasses(Kryo kryo) {
    super.registerClasses(kryo);

    kryo.register(TestData.TestBill.class);
    kryo.register(DummyFactory.class);
    kryo.register(InMemoryPostBillingRepository.class);
  }
}
