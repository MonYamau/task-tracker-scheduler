package ru.monyamau.task_tracker_scheduler.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {
    private final String reportRequestTopic;

    public KafkaConfig(@Value("${kafka.topics.request}") String reportRequestTopic) {
        this.reportRequestTopic = reportRequestTopic;
    }

    @Bean
    public NewTopic createReportResponseTopic() {
        return TopicBuilder.name(reportRequestTopic)
                .replicas(1)
                .partitions(1)
                .build();
    }

    @Bean
    public DefaultErrorHandler defaultErrorHandler() {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(3000, 2));
        errorHandler.addRetryableExceptions();
        return errorHandler;
    }
}