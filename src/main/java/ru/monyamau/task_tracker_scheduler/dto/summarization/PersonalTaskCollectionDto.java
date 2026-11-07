package ru.monyamau.task_tracker_scheduler.dto.summarization;

import java.util.List;

public record PersonalTaskCollectionDto(List<TaskDto> tasks) {
}