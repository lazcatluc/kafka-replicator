package com.endava.replicator.kafka;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class KafkaReplicationConfirmationSubscriber {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReplicationConfirmationSubscriber.class);

    private final long kafkaReplicationTimeout;
    private final int kafkaReplicationConfirmations;
    private final Map<String, CountDownLatch> confirmationHook = new ConcurrentHashMap<>();

    public KafkaReplicationConfirmationSubscriber(@Value("${kafka.replication.timeout.seconds}") long kafkaReplicationTimeout,
                                                  @Value("${kafka.replication.confirmations}") int kafkaReplicationConfirmations) {
        this.kafkaReplicationTimeout = kafkaReplicationTimeout;
        this.kafkaReplicationConfirmations = kafkaReplicationConfirmations;
    }

    @KafkaListener(topics = "${kafka.replication.success.topic}")
    public void receiveSuccess(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {
        confirmationHook.getOrDefault(key, new CountDownLatch(1)).countDown();
    }

    @KafkaListener(topics = "${kafka.replication.failure.topic}")
    public void receiveFailure(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key, @Payload String message) {
        LOGGER.error("Replication error for {}: {}", key, message);
        confirmationHook.getOrDefault(key, new CountDownLatch(1)).countDown();
    }

    public void prepareForConfirmation(KafkaEntityWrapper kafkaEntityWrapper) {
        CountDownLatch confirmationLatch = new CountDownLatch(kafkaReplicationConfirmations);
        String wrapperId = kafkaEntityWrapper.getId();
        confirmationHook.put(wrapperId, confirmationLatch);
    }

    public void waitForConfirmation(KafkaEntityWrapper kafkaEntityWrapper) {
        CountDownLatch confirmationLatch = confirmationHook.get(kafkaEntityWrapper.getId());
        try {
            confirmationLatch.await(kafkaReplicationTimeout, TimeUnit.SECONDS);
            if (confirmationLatch.getCount() > 0) {
                throw new TimeoutException();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void endConfirmation(KafkaEntityWrapper kafkaEntityWrapper) {
        confirmationHook.remove(kafkaEntityWrapper.getId());
    }
}
