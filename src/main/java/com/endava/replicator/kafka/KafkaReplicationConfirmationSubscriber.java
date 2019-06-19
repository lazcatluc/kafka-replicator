package com.endava.replicator.kafka;

import java.time.Duration;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

@Component
public class KafkaReplicationConfirmationSubscriber {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReplicationConfirmationSubscriber.class);

    private final String kafkaReplicationSuccessTopic;
    private final String kafkaReplicationFailureTopic;
    private final long kafkaReplicationTimeout;
    private final ConsumerFactory<String, String> consumerFactory;

    public KafkaReplicationConfirmationSubscriber(@Value("${kafka.replication.success.topic}") String kafkaReplicationSuccessTopic,
                                                  @Value("${kafka.replication.failure.topic}") String kafkaReplicationFailureTopic,
                                                  @Value("${kafka.replication.timeout.seconds}") long kafkaReplicationTimeout,
                                                  ConsumerFactory<String, String> consumerFactory) {
        this.kafkaReplicationSuccessTopic = kafkaReplicationSuccessTopic;
        this.kafkaReplicationFailureTopic = kafkaReplicationFailureTopic;
        this.kafkaReplicationTimeout = kafkaReplicationTimeout;
        this.consumerFactory = consumerFactory;
    }

    public void waitForConfirmation(KafkaEntityWrapper kafkaEntityWrapper) {
        try (Consumer<String, String> consumer = consumerFactory.createConsumer()) {
            String wrapperId = kafkaEntityWrapper.getId();
            consumer.subscribe(Arrays.asList(kafkaReplicationSuccessTopic + "-" + wrapperId,
                    kafkaReplicationFailureTopic + "-" + wrapperId));
            LOGGER.debug("Waiting for replication confirmation of " + kafkaEntityWrapper);
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(kafkaReplicationTimeout));
            if (records.isEmpty()) {
                throw new TimeoutException();
            }
            for (ConsumerRecord<String, String> successRecord : records.records(
                    kafkaReplicationSuccessTopic + "-" + wrapperId)) {
                LOGGER.debug("Found successful replication for {}: {}", kafkaEntityWrapper, successRecord);
            }
            for (ConsumerRecord<String, String> failureRecord : records.records(
                    kafkaReplicationFailureTopic + "-" + wrapperId)) {
                throw new IllegalStateException(failureRecord.value());
            }
        }
    }
}
