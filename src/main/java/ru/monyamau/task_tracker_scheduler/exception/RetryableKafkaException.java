package ru.monyamau.task_tracker_scheduler.exception;

public class RetryableKafkaException extends RuntimeException {
    public RetryableKafkaException(String message, Throwable cause) {
        super(message, cause);
    }
}
