package ru.monyamau.task_tracker_scheduler.infrastructure;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.stereotype.Component;
import ru.monyamau.task_tracker_scheduler.dto.summarization.PersonalTaskCollectionDto;
import ru.monyamau.task_tracker_scheduler.dto.summarization.PersonalTaskReportDto;
import ru.monyamau.task_tracker_scheduler.exception.NonRetryableKafkaException;
import ru.monyamau.task_tracker_scheduler.exception.RetryableKafkaException;

@Component
public class TaskReportRequestProducer {
    private final ReplyingKafkaTemplate<String, PersonalTaskCollectionDto, PersonalTaskReportDto> replyingKafkaTemplate;
    private final String topic;

    public TaskReportRequestProducer(
            ReplyingKafkaTemplate<String, PersonalTaskCollectionDto, PersonalTaskReportDto> replyingKafkaTemplate,
            @Value("${kafka.topics.request}") String topic) {
        this.replyingKafkaTemplate = replyingKafkaTemplate;
        this.topic = topic;
    }

    public PersonalTaskReportDto requestReport(PersonalTaskCollectionDto personalTaskCollectionDto) {
        try {
            ProducerRecord<String, PersonalTaskCollectionDto> record = new ProducerRecord<>(topic, personalTaskCollectionDto);
            ConsumerRecord<String, PersonalTaskReportDto> response = replyingKafkaTemplate.sendAndReceive(record).get();
            return response.value();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NonRetryableKafkaException("Ожидание ответа о запрашиваемом персональном отчёте было прервано", e);
        } catch (Exception e) {
            throw determineCause(e);
        }
    }

    private RuntimeException determineCause(Throwable exception) {
        Throwable duplicateException = exception;
        while (duplicateException != null) {
            if (duplicateException instanceof RetriableException) {
                return new RetryableKafkaException("Допустимо для повторного вызова", exception);
            }
            duplicateException = duplicateException.getCause();
        }
        return new NonRetryableKafkaException("Ошибка при попытке запросить персональный отчёт через Kafka", exception);
    }
}