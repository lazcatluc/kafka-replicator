package com.endava.replicator.kafka;

import java.io.UncheckedIOException;
import java.util.Map;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Component
public class KafkaReplicationSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReplicationSender.class);

    private final String kafkaReplicationTopic;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaReplicationSender(@Value("${kafka.replication.topic}") String kafkaReplicationTopic,
                                  ObjectMapper objectMapper, KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaReplicationTopic = kafkaReplicationTopic;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void replicate(Object message) {
        try {
            Map map = objectMapper.convertValue(message, Map.class);
            String entityClassName = message.getClass().getCanonicalName();
            ListenableFuture<SendResult<String, String>> future = kafkaTemplate
                    .send(kafkaReplicationTopic, objectMapper.writeValueAsString(new KafkaEntityWrapper(entityClassName, map)));
            future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
                @Override
                public void onSuccess(SendResult<String, String> result) {
                    LOGGER.info("Sent " + message + " with offset " + result.getRecordMetadata().offset() + " for replication");
                }

                @Override
                public void onFailure(Throwable ex) {
                    LOGGER.error("Unable to send " + message + " for replication", ex);
                }
            });
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
