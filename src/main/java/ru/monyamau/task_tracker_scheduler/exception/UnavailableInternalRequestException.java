package ru.monyamau.task_tracker_scheduler.exception;

public class UnavailableInternalRequestException extends RuntimeException {
    public UnavailableInternalRequestException(String message, Throwable cause) {
        super(message, cause);
    }
}