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
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;
import ru.monyamau.task_tracker_scheduler.dto.mailing.PersonalReportLetterDto;
import ru.monyamau.task_tracker_scheduler.dto.summarization.PersonalTaskCollectionDto;
import ru.monyamau.task_tracker_scheduler.dto.summarization.PersonalTaskReportDto;

import java.time.Duration;
import java.util.Map;

@Configuration
public class KafkaConfig {
    private static final long RPC_REPLY_TIMEOUT_SEC = 120L;

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
    public ConsumerFactory<String, PersonalTaskReportDto> replyConsumerFactory(KafkaProperties kafkaProperties) {
        return new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PersonalTaskReportDto> replyListenerContainerFactory(
            @Qualifier("replyConsumerFactory") ConsumerFactory<String, PersonalTaskReportDto> consumerFactory,
            @Qualifier("defaultErrorHandler") DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, PersonalTaskReportDto> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    @Bean
    public ConcurrentMessageListenerContainer<String, PersonalTaskReportDto> replyContainer(
            @Qualifier("replyListenerContainerFactory")
            ConcurrentKafkaListenerContainerFactory<String, PersonalTaskReportDto> factory,
            @Value("${kafka.topics.response}") String responseTopic,
            @Value("${kafka.reply.group-id}") String replyGroupId) {
        ConcurrentMessageListenerContainer<String, PersonalTaskReportDto> container =
                factory.createContainer(responseTopic);
        container.getContainerProperties().setGroupId(replyGroupId);
        return container;
    }

    @Bean
    public ReplyingKafkaTemplate<String, PersonalTaskCollectionDto, PersonalTaskReportDto> reportReplyingKafkaTemplate(
            @Qualifier("reportRequestProducerFactory")
            ProducerFactory<String, PersonalTaskCollectionDto> producerFactory,
            @Qualifier("replyContainer")
            ConcurrentMessageListenerContainer<String, PersonalTaskReportDto> replyContainer) {
        ReplyingKafkaTemplate<String, PersonalTaskCollectionDto, PersonalTaskReportDto> template =
                new ReplyingKafkaTemplate<>(producerFactory, replyContainer);
        template.setDefaultReplyTimeout(Duration.ofSeconds(RPC_REPLY_TIMEOUT_SEC));
        return template;
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
        return new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 0L));
    }
}