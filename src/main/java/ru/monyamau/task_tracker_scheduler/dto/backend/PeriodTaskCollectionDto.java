package ru.monyamau.task_tracker_scheduler.dto.backend;

import ru.monyamau.task_tracker_scheduler.dto.TaskDto;

import java.util.List;

public record PeriodTaskCollectionDto(List<TaskDto> tasks) {
}