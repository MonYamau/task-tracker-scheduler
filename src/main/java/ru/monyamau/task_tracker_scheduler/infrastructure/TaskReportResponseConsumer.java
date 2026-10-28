package ru.monyamau.task_tracker_scheduler.infrastructure;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TaskReportResponseConsumer {

    @KafkaListener(topics = "${kafka.topics.response}")
    public void consume() {
    }
}