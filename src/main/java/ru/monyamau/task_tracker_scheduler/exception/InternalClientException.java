package ru.monyamau.task_tracker_scheduler.exception;

public class InternalClientException extends RuntimeException {
    public InternalClientException(String message, Throwable cause) {
        super(message, cause);
    }
}