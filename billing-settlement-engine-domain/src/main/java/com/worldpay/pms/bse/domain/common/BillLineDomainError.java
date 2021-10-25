package com.worldpay.pms.bse.domain.common;

import com.worldpay.pms.pce.common.DomainError;
import io.vavr.collection.List;
import io.vavr.collection.Seq;
import lombok.NonNull;
import lombok.Value;

@Value
public class BillLineDomainError {

  String billLineId;
  @NonNull
  DomainError domainError;

  public static BillLineDomainError from(String billLineId, DomainError domainError) {
    return new BillLineDomainError(billLineId, domainError);
  }

  public static BillLineDomainError from(DomainError domainError) {
    return new BillLineDomainError(null, domainError);
  }

  public Seq<BillLineDomainError> toSeq() {
    return List.of(this);
  }
}
