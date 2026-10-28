package ru.monyamau.task_tracker_scheduler.exception;

public class CustomKafkaException extends RuntimeException {
    public CustomKafkaException(String message, Throwable cause) {
        super(message, cause);
    }
}