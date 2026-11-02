package ru.monyamau.task_tracker_scheduler.dto.summarization;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;

public record PersonalTaskReportDto(
        @NotBlank(message = "Электронная почта не может отсутствовать")
        @Email(message = "Невалидный формат электронной почты")
        String userEmail,
        @NotBlank(message = "Отчёт по задачам пользователя не может отсутствовать или быть пустым")
        String personalReport) {
}