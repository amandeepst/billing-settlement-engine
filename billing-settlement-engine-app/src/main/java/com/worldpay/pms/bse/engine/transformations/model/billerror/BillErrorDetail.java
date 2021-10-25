package com.worldpay.pms.bse.engine.transformations.model.billerror;

import static org.apache.commons.lang3.StringUtils.left;

import com.worldpay.pms.pce.common.DomainError;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import org.apache.commons.lang.exception.ExceptionUtils;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@With
@Builder
public class BillErrorDetail {

  public static final int MAX_ERROR_LEN = 2048;
  public static final int MAX_STACKTRACE_LEN = 4000;
  public static final String UNHANDLED_EXCEPTION_CODE = "UNHANDLED_EXCEPTION";

  String billLineId;
  String code;
  String message;
  String stackTrace;

  public static BillErrorDetail of(String billLineId, Throwable ex) {
    return BillErrorDetail.of(billLineId, UNHANDLED_EXCEPTION_CODE, ex.getMessage(), ExceptionUtils.getStackTrace(ex));
  }

  public static BillErrorDetail of(String billLineId, DomainError error) {
    return BillErrorDetail.of(billLineId, error.getCode(), error.getMessage(), null);
  }

  public static BillErrorDetail of(DomainError error) {
    return BillErrorDetail.of(null, error.getCode(), error.getMessage(), null);
  }

  private static BillErrorDetail of(String billLineId, String code, String reason, String stackTrace) {
    return new BillErrorDetail(billLineId, left(code, MAX_ERROR_LEN), left(reason, MAX_ERROR_LEN),
        left(stackTrace, MAX_STACKTRACE_LEN));
  }
}
