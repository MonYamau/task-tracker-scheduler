package ru.monyamau.task_tracker_scheduler.infrastructure;

import org.apache.kafka.common.errors.RetriableException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import ru.monyamau.task_tracker_scheduler.dto.mailing.PersonalReportLetterDto;
import ru.monyamau.task_tracker_scheduler.exception.NonRetryableKafkaException;
import ru.monyamau.task_tracker_scheduler.exception.RetryableKafkaException;

import java.util.concurrent.ExecutionException;

@Component
public class LetterSendingProducer {
    private final KafkaTemplate<String, PersonalReportLetterDto> kafkaTemplate;
    private final String topic;

    public LetterSendingProducer(KafkaTemplate<String, PersonalReportLetterDto> kafkaTemplate, @Value("${kafka.topics.mailing}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publishLetterTask(PersonalReportLetterDto personalReportLetterDto) {
        try {
            kafkaTemplate.send(topic, personalReportLetterDto).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NonRetryableKafkaException("Попытка публикации задачи для отправки письма в Kafka была прервана", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw determineCause(cause);
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
        return new NonRetryableKafkaException("Ошибка при попытке опубликовать задачу для отправки письма в Kafka", exception);
    }
}