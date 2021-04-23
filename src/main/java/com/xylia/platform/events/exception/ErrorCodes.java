package com.xylia.platform.events.exception;

/**
 * Error codes enum constants
 *
 * @author Rajesh Iyer
 */
public enum ErrorCodes {

    SUCCESS("PUBLISHED SUCCESSFULLY!", "1000"),
    SERVICE_DOWN("PUBLISHER SERVICE RUNTIME ERROR", "1030"),
    INTERNAL_ERROR("INTERNAL_ERROR", "1032"),
    PAYLOAD_EMPTY("PAYLOAD_EMPTY", "1050");

    private final String message;
    private final String errorCode;

    ErrorCodes(String message, String errorCode) {
        this.message = message;
        this.errorCode = errorCode;
    }

    public String getMessage() {
        return message;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getDescription(String customMessage) {
        return "ErrorCode: " + errorCode + " Description: " + message + " Message: " + customMessage;
    }
}
