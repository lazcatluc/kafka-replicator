package com.endava.replicator.kafka;

import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaReplicationSuccessSender {
    private final String kafkaReplicationSuccessTopic;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final long kafkaReplicationTimeout;
    private final ObjectMapper objectMapper;

    public KafkaReplicationSuccessSender(@Value("${kafka.replication.success.topic}") String kafkaReplicationSuccessTopic,
                                         KafkaTemplate<String, String> kafkaTemplate,
                                         @Value("${kafka.replication.timeout.seconds}") long kafkaReplicationTimeout,
                                         ObjectMapper objectMapper) {
        this.kafkaReplicationSuccessTopic = kafkaReplicationSuccessTopic;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaReplicationTimeout = kafkaReplicationTimeout;
        this.objectMapper = objectMapper;
    }

    public void sendReplicationSuccess(KafkaEntityWrapper kafkaEntityWrapper)
            throws InterruptedException, ExecutionException, TimeoutException {
        try {
            String ok = objectMapper.writeValueAsString(
                    new ReplicationStatus(InetAddress.getLocalHost().getHostName(), "OK"));
            kafkaTemplate.send(kafkaReplicationSuccessTopic, kafkaEntityWrapper.getId(), ok)
                    .get(kafkaReplicationTimeout, TimeUnit.SECONDS);
        } catch (UnknownHostException | JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
