package ru.monyamau.task_tracker_scheduler.infrastructure;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.monyamau.task_tracker_scheduler.dto.summarization.PersonalTaskReportDto;
import ru.monyamau.task_tracker_scheduler.service.ReportMailingService;

@Component
public class TaskReportResponseConsumer {
    private final ReportMailingService reportMailingService;

    public TaskReportResponseConsumer(ReportMailingService reportMailingService) {
        this.reportMailingService = reportMailingService;
    }

    @KafkaListener(topics = "${kafka.topics.response}")
    public void consume(PersonalTaskReportDto personalTaskReportDto) {
        reportMailingService.sendReportToUser(personalTaskReportDto);
    }
}