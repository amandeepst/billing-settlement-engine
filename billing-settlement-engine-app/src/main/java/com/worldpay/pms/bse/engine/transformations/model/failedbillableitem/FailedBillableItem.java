package com.worldpay.pms.bse.engine.transformations.model.failedbillableitem;

import static com.worldpay.pms.bse.engine.transformations.TransformationUtils.getCurrentDate;
import static com.worldpay.pms.bse.engine.transformations.model.FieldConstants.IGNORED_RETRY_COUNT;
import static org.apache.commons.lang3.StringUtils.left;

import com.worldpay.pms.bse.domain.model.PendingBill;
import com.worldpay.pms.bse.engine.transformations.model.billableitem.BillableItemRow;
import com.worldpay.pms.pce.common.DomainError;
import com.worldpay.pms.spark.core.ErrorEvent;
import io.vavr.collection.Seq;
import java.sql.Date;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang.exception.ExceptionUtils;

@Data
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class FailedBillableItem implements ErrorEvent {

  public static final String DUPLICATED_STATUS = "DUPE";
  public static final String ERROR_STATUS = "EROR";
  public static final String IGNORED_STATUS = "IGNR";
  public static final String UNHANDLED_EXCEPTION_CODE = "UNHANDLED_EXCEPTION";
  public static final String UNKNOWN_ERROR_CODE = "UNKNOWN ERROR";
  private static final int MAX_ERROR_LEN = 4000;
  private static final int MAX_STACK_TRACE_LEN = 4000;

  private String billItemId;
  private Date firstFailureOn;
  private int retryCount;
  private String code;
  private String status;
  private String reason;
  private String stackTrace;
  private Date billableItemIlmDt;
  private Date fixedDt;

  public static FailedBillableItem of(BillableItemRow eventRow, Throwable ex) {
    return FailedBillableItem.of(eventRow,
        DomainError.of(UNHANDLED_EXCEPTION_CODE, UNHANDLED_EXCEPTION_CODE),
        ex.getMessage(),
        ExceptionUtils.getStackTrace(ex));
  }

  public static FailedBillableItem of(BillableItemRow itemRow, Seq<DomainError> errors) {
    return FailedBillableItem.of(itemRow, errors, "");
  }

  public static FailedBillableItem of(BillableItemRow itemRow, Seq<DomainError> errors, String stackTrace) {
    DomainError firstDomainError = errors.headOption().getOrElse(new DomainError(UNKNOWN_ERROR_CODE, UNKNOWN_ERROR_CODE));
    return FailedBillableItem.of(itemRow,
        firstDomainError,
        DomainError.concatAll(errors).getMessage(),
        stackTrace);
  }

  public static FailedBillableItem fixed(PendingBill pendingBill) {
    return new FailedBillableItem(
        pendingBill.getBillableItemId(),
        pendingBill.getBillableItemFirstFailureOn(),
        pendingBill.getBillableItemRetryCount() + 1,
        " ",
        "COMP",
        " ",
        " ",
        pendingBill.getBillableItemIlmDate(),
        getCurrentDate()
    );
  }

  private static FailedBillableItem of(BillableItemRow itemRow, DomainError firstDomainError,
      String reason, String stackTrace) {
    return new FailedBillableItem(
        itemRow.getBillableItemId(),
        itemRow.getFirstFailureOn() == null ? getCurrentDate() : itemRow.getFirstFailureOn(),
        itemRow.getRetryCount() + 1,
        left(firstDomainError.getCode(), MAX_ERROR_LEN),
        ERROR_STATUS,
        left(reason, MAX_ERROR_LEN),
        left(stackTrace, MAX_STACK_TRACE_LEN),
        itemRow.getIlmDate(),
        null
    );
  }

  @Override
  public boolean isIgnored() {
    return retryCount == IGNORED_RETRY_COUNT;
  }

  @Override
  public boolean isFirstFailure() {
    return getFirstFailureOn() == null || getFirstFailureOn().toLocalDate().isEqual(getCurrentDate().toLocalDate());
  }
}
