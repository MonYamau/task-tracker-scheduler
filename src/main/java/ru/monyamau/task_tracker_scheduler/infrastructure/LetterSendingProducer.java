package ru.monyamau.task_tracker_scheduler.infrastructure;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import ru.monyamau.task_tracker_scheduler.dto.mailing.PersonalReportLetterDto;
import ru.monyamau.task_tracker_scheduler.exception.CustomKafkaException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
            kafkaTemplate.send(topic, personalReportLetterDto).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CustomKafkaException("Попытка публикации задачи для отправки письма в Kafka была прервана", e);
        } catch (TimeoutException e) {
            throw new CustomKafkaException("Не удалось получить подтверждение от Kafka о получении задачи для отправки письма", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw new CustomKafkaException("Не удалось опубликовать задачу для отправки письма в Kafka", cause);
        } catch (Exception e) {
            throw new CustomKafkaException("Ошибка при попытке опубликовать задачу для отправки письма в Kafka", e);
        }
    }
}