package com.worldpay.pms.bse.engine.transformations;

import com.worldpay.pms.bse.engine.transformations.model.failedbillableitem.FailedBillableItem;
import com.worldpay.pms.bse.engine.transformations.model.pendingbill.PendingBillRow;
import io.vavr.control.Either;
import lombok.Value;

/**
 * Wrapper type for an Either since is not possible to serialize type signatures that contain generics in an easy manner.
 */
@Value
public class PendingBillResult {

  PendingBillRow success;
  FailedBillableItem failure;

  public PendingBillResult(Either<FailedBillableItem, PendingBillRow> successOrFailure) {
    this.failure = successOrFailure.isLeft() ? successOrFailure.getLeft() : null;
    this.success = successOrFailure.isRight() ? successOrFailure.get() : null;
  }

  public boolean isSuccess() {
    return success != null;
  }

  public boolean isFailure() {
    return !isSuccess();
  }

  public boolean isPreviouslyFailedAndFixed() {
    return isSuccess() && (this.getSuccess().getBillableItemFirstFailureOn() != null);
  }

}
