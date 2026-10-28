package ru.monyamau.task_tracker_scheduler.util;

import lombok.experimental.UtilityClass;
import ru.monyamau.task_tracker_scheduler.dto.mailing.PersonalReportLetterDto;

@UtilityClass
public final class LetterCreator {
    private static final String DEFAULT_TITLE = "Отчёт по задачам за прошедший день";

    public static PersonalReportLetterDto createReportLetter(String recipient, String report) {
        return new PersonalReportLetterDto(recipient, DEFAULT_TITLE, report);
    }
}