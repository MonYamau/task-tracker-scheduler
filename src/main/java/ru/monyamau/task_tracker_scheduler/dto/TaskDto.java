package ru.monyamau.task_tracker_scheduler.dto;

import java.time.OffsetDateTime;

public record TaskDto(Integer id, String title, String text, boolean isReady, OffsetDateTime plannedCompletionAt,
                      OffsetDateTime completedAt) {
}