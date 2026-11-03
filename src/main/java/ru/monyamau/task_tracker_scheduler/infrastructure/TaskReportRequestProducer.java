package ru.monyamau.task_tracker_scheduler.infrastructure;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import ru.monyamau.task_tracker_scheduler.dto.summarization.PersonalTaskCollectionDto;
import ru.monyamau.task_tracker_scheduler.exception.NonRetryableKafkaException;

import java.util.concurrent.ExecutionException;

@Component
public class TaskReportRequestProducer {
    private final KafkaTemplate<String, PersonalTaskCollectionDto> kafkaTemplate;
    private final String topic;

    public TaskReportRequestProducer(KafkaTemplate<String, PersonalTaskCollectionDto> kafkaTemplate, @Value("${kafka.topics.request}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publishReportRequest(PersonalTaskCollectionDto personalTaskCollectionDto) {
        try {
            kafkaTemplate.send(topic, personalTaskCollectionDto).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NonRetryableKafkaException("Попытка публикации задачи о запросе отчёта в Kafka была прервана", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw new NonRetryableKafkaException("Не удалось опубликовать задачу о запросе отчёта в Kafka", cause);
        } catch (Exception e) {
            throw new NonRetryableKafkaException("Ошибка при попытке опубликовать задачу о запросе отчёта в Kafka", e);
        }
    }
}