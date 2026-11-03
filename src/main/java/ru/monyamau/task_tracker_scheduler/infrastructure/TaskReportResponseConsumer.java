package ru.monyamau.task_tracker_scheduler.infrastructure;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import ru.monyamau.task_tracker_scheduler.dto.summarization.PersonalTaskReportDto;
import ru.monyamau.task_tracker_scheduler.service.ReportMailingService;

@Slf4j
@Component
public class TaskReportResponseConsumer {
    private final ReportMailingService reportMailingService;

    public TaskReportResponseConsumer(ReportMailingService reportMailingService) {
        this.reportMailingService = reportMailingService;
    }

    @KafkaListener(topics = "${kafka.topics.response}")
    public void consume(@Valid @Payload PersonalTaskReportDto personalTaskReportDto) {
        reportMailingService.sendReportToUser(personalTaskReportDto);
    }
}