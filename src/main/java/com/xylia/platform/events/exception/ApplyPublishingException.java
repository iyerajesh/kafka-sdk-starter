package com.xylia.platform.events.exception;

public class ApplyPublishingException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    private ErrorCodes errorCode;

    public ApplyPublishingException() {
        super();
        errorCode = ErrorCodes.INTERNAL_ERROR;
    }

    public ApplyPublishingException(String message, ErrorCodes errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public ApplyPublishingException(String message, Throwable cause) {
        super(message, cause);
    }

    public ApplyPublishingException(String message, Throwable cause, ErrorCodes errorCode) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ApplyPublishingException(String message) {
        super(message);
    }

    public ApplyPublishingException(Throwable cause) {
        super(cause);
    }

    public ErrorCodes getErrorCode() {
        return errorCode;
    }
}
