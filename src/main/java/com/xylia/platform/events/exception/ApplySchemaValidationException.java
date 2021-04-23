package com.xylia.platform.events.exception;

public class ApplySchemaValidationException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    private ErrorCodes errorCode;

    public ApplySchemaValidationException() {
        super();
        errorCode = ErrorCodes.INTERNAL_ERROR;
    }

    public ApplySchemaValidationException(String message, ErrorCodes errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public ApplySchemaValidationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ApplySchemaValidationException(String message, Throwable cause, ErrorCodes errorCode) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ApplySchemaValidationException(String message) {
        super(message);
    }

    public ApplySchemaValidationException(Throwable cause) {
        super(cause);
    }

    public ErrorCodes getErrorCode() {
        return errorCode;
    }
}
