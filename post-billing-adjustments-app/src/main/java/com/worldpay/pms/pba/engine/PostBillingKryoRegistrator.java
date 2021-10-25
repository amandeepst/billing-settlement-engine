package com.worldpay.pms.pba.engine;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsEmptyListSerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsEmptyMapSerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonListSerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonMapSerializer;
import com.worldpay.pms.pba.domain.model.Adjustment;
import com.worldpay.pms.pba.domain.model.Bill;
import com.worldpay.pms.pba.domain.model.BillAdjustment;
import com.worldpay.pms.pba.engine.transformations.PostBillingRepositoryFactory;
import com.worldpay.pms.pba.engine.transformations.WafProcessingServiceFactory;
import com.worldpay.pms.spark.KryoConfiguration;
import com.worldpay.pms.spark.core.DataSource;
import com.worldpay.pms.spark.core.jdbc.JdbcConfiguration;
import java.math.BigDecimal;
import java.util.Collections;

public class PostBillingKryoRegistrator extends KryoConfiguration {

  @Override
  public void registerClasses(Kryo kryo) {
    super.registerClasses(kryo);

    // datasource
    kryo.register(DataSource.class);
    kryo.register(JdbcConfiguration.class);

    //domain
    kryo.register(BigDecimal.class);
    kryo.register(BillAdjustment.class);
    kryo.register(Adjustment.class);
    kryo.register(Bill.class);
    kryo.register(WafProcessingServiceFactory.class);
    kryo.register(PostBillingConfiguration.Store.class);
    kryo.register(PostBillingRepositoryFactory.class);
    kryo.register(java.util.HashMap.class);

    kryo.register(Collections.emptyList().getClass(), new CollectionsEmptyListSerializer());
    kryo.register(Collections.singletonList("").getClass(), new CollectionsSingletonListSerializer());
    kryo.register(Collections.singletonMap("", "").getClass(), new CollectionsSingletonMapSerializer());
    kryo.register(Collections.emptyMap().getClass(), new CollectionsEmptyMapSerializer());
    kryo.register(scala.collection.convert.Wrappers$.class);
    kryo.register(org.apache.spark.unsafe.types.UTF8String.class);

    kryo.register(io.vavr.Tuple2.class);
    kryo.register(org.apache.spark.sql.types.Decimal.class);
    kryo.register(scala.math.BigDecimal.class);
    kryo.register(java.math.MathContext.class);
    kryo.register(java.math.RoundingMode.class);

    kryo.register(com.worldpay.pms.pba.domain.model.WithholdFunds.class);
    kryo.register(com.worldpay.pms.pba.domain.model.WithholdFundsBalance.class);
    kryo.register(com.worldpay.pms.pba.engine.model.input.BillRow.class);
    kryo.register(com.worldpay.pms.pba.engine.model.output.BillAdjustmentAccountingRow.class);

  }
}
