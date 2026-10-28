package ru.monyamau.task_tracker_scheduler.dto.backend;

import java.time.OffsetDateTime;

public record PeriodTimeDto(OffsetDateTime from, OffsetDateTime to) {
}