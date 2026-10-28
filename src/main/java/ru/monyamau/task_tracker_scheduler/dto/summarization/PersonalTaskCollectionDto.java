package ru.monyamau.task_tracker_scheduler.dto.summarization;

import ru.monyamau.task_tracker_scheduler.dto.TaskDto;

import java.util.List;

public record PersonalTaskCollectionDto(Integer userId, List<TaskDto> tasks) {
}