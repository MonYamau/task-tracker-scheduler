package ru.monyamau.task_tracker_scheduler.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;
import ru.monyamau.task_tracker_scheduler.dto.mailing.PersonalReportLetterDto;
import ru.monyamau.task_tracker_scheduler.dto.summarization.PersonalTaskCollectionDto;
import ru.monyamau.task_tracker_scheduler.dto.summarization.PersonalTaskReportDto;
import ru.monyamau.task_tracker_scheduler.exception.NonRetryableKafkaException;

import java.util.Map;

@Configuration
public class KafkaConfig {
    private static final long MS_INTERVAL = 3000L;
    private static final long NUM_OF_RETRIES = 2L;

    private final String reportRequestTopic;
    private final String dltReportResponseTopic;

    public KafkaConfig(@Value("${kafka.topics.request}") String reportRequestTopic,
                       @Value("${kafka.topics.dlt-response}") String dltReportResponseTopic) {
        this.reportRequestTopic = reportRequestTopic;
        this.dltReportResponseTopic = dltReportResponseTopic;
    }

    @Bean
    public NewTopic createReportRequestTopic() {
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
    public ProducerFactory<String, PersonalTaskCollectionDto> reportRequestProducerFactory(KafkaProperties kafkaProperties) {
        return new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties());
    }

    @Bean
    public KafkaTemplate<String, PersonalTaskCollectionDto> reportRequestKafkaTemplate(
            @Qualifier("reportRequestProducerFactory") ProducerFactory<String, PersonalTaskCollectionDto> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ProducerFactory<String, PersonalReportLetterDto> letterDtoProducerFactory(KafkaProperties kafkaProperties) {
        return new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties());
    }

    @Bean
    public KafkaTemplate<String, PersonalReportLetterDto> letterSendingKafkaTemplate(
            @Qualifier("letterDtoProducerFactory") ProducerFactory<String, PersonalReportLetterDto> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ProducerFactory<String, Object> dltProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> producerProperties = kafkaProperties.buildProducerProperties();
        DelegatingByTypeSerializer typeSerializer = new DelegatingByTypeSerializer(
                Map.of(
                        byte[].class, new ByteArraySerializer(),
                        PersonalTaskReportDto.class, new JacksonJsonSerializer<>()
                )
        );
        return new DefaultKafkaProducerFactory<>(producerProperties, new StringSerializer(), typeSerializer);
    }

    @Bean
    public KafkaTemplate<String, Object> dltKafkaTemplate(
            @Qualifier("dltProducerFactory") ProducerFactory<String, Object> dltProducerFactory) {
        return new KafkaTemplate<>(dltProducerFactory);
    }

    @Bean
    public DefaultErrorHandler defaultErrorHandler(
            @Qualifier("dltKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition(dltReportResponseTopic, record.partition()));
        recoverer.setThrowIfNoDestinationReturned(true);
        recoverer.setLogRecoveryRecord(true);
        recoverer.setFailIfSendResultIsError(true);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer,
                new FixedBackOff(MS_INTERVAL, NUM_OF_RETRIES));
        errorHandler.addNotRetryableExceptions(NonRetryableKafkaException.class);
        return errorHandler;
    }
}