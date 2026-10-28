package ru.monyamau.task_tracker_scheduler.dto.backend;

public record FullTaskDto(String title, String text, boolean isReady, String ownerEmail) {
}