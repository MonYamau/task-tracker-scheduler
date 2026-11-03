package ru.monyamau.task_tracker_scheduler.exception;

public class NonRetryableKafkaException extends RuntimeException {
    public NonRetryableKafkaException(String message, Throwable cause) {
        super(message, cause);
    }
}