package com.endava.replicator.kafka;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaReplicationReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReplicationReceiver.class);

    private final ObjectMapper objectMapper;
    private final List<? extends ReplicatedJpaRepository<?, ?>> myEntitiesRepositories;

    public KafkaReplicationReceiver(ObjectMapper objectMapper,
                                    List<? extends ReplicatedJpaRepository<?, ?>> myEntitiesRepositories) {
        this.objectMapper = objectMapper;
        this.myEntitiesRepositories = myEntitiesRepositories;
    }

    @SuppressWarnings("unchecked")
    @KafkaListener(topics = "${kafka.replication.topic}")
    public void listen(String message) {
        LOGGER.info("Received " + message + " for replication");
        try {
            KafkaEntityWrapper kafkaEntityWrapper = objectMapper.readValue(message, KafkaEntityWrapper.class);
            Class<?> entityClass = Class.forName(kafkaEntityWrapper.getEntityClassName());
            Object entity = objectMapper.convertValue(kafkaEntityWrapper.getEntity(), entityClass);
            String repositoryName = entityClass.getSimpleName() + "Repository";
            ReplicatedJpaRepository replicatedJpaRepository = myEntitiesRepositories.stream().filter(repository ->
                    Arrays.stream(repository.getClass().getGenericInterfaces()).anyMatch(type ->
                            type.getTypeName().endsWith("." + repositoryName))).findAny()
                    .orElseThrow(() -> new IllegalArgumentException("Cannot find repository for class " + entityClass));
            replicatedJpaRepository.saveWithoutReplicating(entity);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
