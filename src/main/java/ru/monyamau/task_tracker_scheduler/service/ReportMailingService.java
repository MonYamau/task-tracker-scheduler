package ru.monyamau.task_tracker_scheduler.service;

import org.springframework.stereotype.Service;
import ru.monyamau.task_tracker_scheduler.dto.mailing.PersonalReportLetterDto;
import ru.monyamau.task_tracker_scheduler.infrastructure.LetterSendingProducer;
import ru.monyamau.task_tracker_scheduler.util.LetterCreator;

@Service
public class ReportMailingService {
    private final LetterSendingProducer letterSendingProducer;

    public ReportMailingService(LetterSendingProducer letterSendingProducer) {
        this.letterSendingProducer = letterSendingProducer;
    }

    public void sendReportToUser(String email, String personalReport) {
        PersonalReportLetterDto reportLetter = LetterCreator
                .createReportLetter(email, personalReport);
        letterSendingProducer.publishLetterTask(reportLetter);
    }
}