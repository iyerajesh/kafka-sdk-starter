package com.xylia.platform.events.exception;

public class ApplyConsumerException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    private ErrorCodes errorCode;

    public ApplyConsumerException() {
        super();
        errorCode = ErrorCodes.INTERNAL_ERROR;
    }

    public ApplyConsumerException(String message, ErrorCodes errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public ApplyConsumerException(String message, Throwable cause) {
        super(message, cause);
    }

    public ApplyConsumerException(String message, Throwable cause, ErrorCodes errorCode) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ApplyConsumerException(String message) {
        super(message);
    }

    public ApplyConsumerException(Throwable cause) {
        super(cause);
    }

    public ErrorCodes getErrorCode() {
        return errorCode;
    }
}
