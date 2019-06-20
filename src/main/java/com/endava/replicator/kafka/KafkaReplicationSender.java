package com.endava.replicator.kafka;

import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.persistence.Id;
import javax.transaction.Transactional;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.util.ReflectionUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Component
public class KafkaReplicationSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReplicationSender.class);

    private final String kafkaReplicationTopic;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final long kafkaReplicationTimeout;
    private final KafkaReplicationConfirmationSubscriber kafkaReplicationConfirmationSubscriber;

    public KafkaReplicationSender(@Value("${kafka.replication.topic}") String kafkaReplicationTopic,
                                  @Value("${kafka.replication.timeout.seconds}") long kafkaReplicationTimeout,
                                  ObjectMapper objectMapper, KafkaTemplate<String, String> kafkaTemplate,
                                  KafkaReplicationConfirmationSubscriber kafkaReplicationConfirmationSubscriber) {
        this.kafkaReplicationTopic = kafkaReplicationTopic;
        this.kafkaReplicationTimeout = kafkaReplicationTimeout;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaReplicationConfirmationSubscriber = kafkaReplicationConfirmationSubscriber;
    }

    @Transactional
    public void replicate(String operation, Object message) {
        try {
            LOGGER.debug("Preparing {} for {} replication", message, operation);
            Map map = objectMapper.convertValue(message, Map.class);
            String entityClassName = message.getClass().getCanonicalName();
            KafkaEntityWrapper kafkaEntityWrapper = new KafkaEntityWrapper(entityClassName, map);
            String valueAsString = objectMapper.writeValueAsString(
                    new KafkaReplicationWrapper(operation, kafkaEntityWrapper));
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCompletion(int status) {
                    if (status == STATUS_COMMITTED) {
                        try {
                            String kafkaKey = findKafkaKey(message);
                            LOGGER.debug("Sending [{} : {}] to replication", kafkaKey, valueAsString);
                            kafkaTemplate.send(kafkaReplicationTopic, kafkaKey, valueAsString)
                                    .get(kafkaReplicationTimeout, TimeUnit.SECONDS);
                            kafkaReplicationConfirmationSubscriber.waitForConfirmation(kafkaEntityWrapper);
                        } catch (InterruptedException | ExecutionException | TimeoutException e) {
                            throw new IllegalStateException(e);
                        }
                    } else {
                        LOGGER.debug("Not sending {} to replication because transaction is in status {}", valueAsString, status);
                    }
                }
            });
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String findKafkaKey(Object message) {
        Field idField = ReflectionUtils.findField(message.getClass(),
                new ReflectionUtils.AnnotationFieldFilter(Id.class));
        if (idField != null) {
            org.springframework.util.ReflectionUtils.makeAccessible(idField);
            Object field = org.springframework.util.ReflectionUtils.getField(idField, message);
            if (field != null) {
                return field.toString();
            }
        }
        return String.valueOf(message.hashCode());
    }
}
