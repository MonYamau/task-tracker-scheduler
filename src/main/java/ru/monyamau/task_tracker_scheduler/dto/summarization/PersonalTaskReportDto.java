package ru.monyamau.task_tracker_scheduler.dto.summarization;

import jakarta.validation.constraints.NotBlank;

public record PersonalTaskReportDto(
        @NotBlank(message = "Отчёт по задачам пользователя не может отсутствовать или быть пустым")
        String personalReport) {
}