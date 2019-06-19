package com.endava.replicator.kafka;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaReplicationFailureSender {
    private final String kafkaReplicationFailureTopic;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final long kafkaReplicationTimeout;
    private final ObjectMapper objectMapper;

    public KafkaReplicationFailureSender(@Value("${kafka.replication.failure.topic}") String kafkaReplicationFailureTopic,
                                         KafkaTemplate<String, String> kafkaTemplate,
                                         @Value("${kafka.replication.timeout.seconds}") long kafkaReplicationTimeout,
                                         ObjectMapper objectMapper) {
        this.kafkaReplicationFailureTopic = kafkaReplicationFailureTopic;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaReplicationTimeout = kafkaReplicationTimeout;
        this.objectMapper = objectMapper;
    }

    public void sendReplicationFailure(KafkaEntityWrapper kafkaEntityWrapper, Throwable t)
            throws InterruptedException, ExecutionException, TimeoutException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             PrintStream printStream = new PrintStream(baos)) {
            t.printStackTrace(printStream);
            String valueAsString = objectMapper.writeValueAsString(
                    new ReplicationStatus(InetAddress.getLocalHost().getHostName(),
                            baos.toString(StandardCharsets.UTF_8.toString())));
            kafkaTemplate.send(kafkaReplicationFailureTopic + "-" + kafkaEntityWrapper.getId(), valueAsString)
                    .get(kafkaReplicationTimeout, TimeUnit.SECONDS);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
