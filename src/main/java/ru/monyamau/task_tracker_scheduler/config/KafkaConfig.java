package ru.monyamau.task_tracker_scheduler.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import ru.monyamau.task_tracker_scheduler.dto.summarization.PersonalTaskReportDto;

@Configuration
public class KafkaConfig {
    private final String reportRequestTopic;
    private final String dltReportResponseTopic;

    public KafkaConfig(@Value("${kafka.topics.request}") String reportRequestTopic,
                       @Value("${kafka.topics.dlt-response}") String dltReportResponseTopic) {
        this.reportRequestTopic = reportRequestTopic;
        this.dltReportResponseTopic = dltReportResponseTopic;
    }

    @Bean
    public NewTopic createReportResponseTopic() {
        return TopicBuilder.name(reportRequestTopic)
                .replicas(1)
                .partitions(1)
                .build();
    }

    @Bean
    public NewTopic createDltReportResponseTopic() {
        return TopicBuilder.name(dltReportResponseTopic)
                .replicas(1)
                .partitions(1)
                .build();
    }

    @Bean
    public DefaultErrorHandler defaultErrorHandler(KafkaTemplate<String, PersonalTaskReportDto> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition(dltReportResponseTopic, record.partition()));
        recoverer.setThrowIfNoDestinationReturned(true);
        recoverer.setLogRecoveryRecord(true);
        recoverer.setFailIfSendResultIsError(true);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(3000, 2));
        errorHandler.addRetryableExceptions();
        return errorHandler;
    }
}