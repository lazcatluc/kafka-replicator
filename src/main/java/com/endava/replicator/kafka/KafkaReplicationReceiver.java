package com.endava.replicator.kafka;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

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

    @KafkaListener(topics = "${kafka.replication.topic}")
    public void listen(String message) {
        LOGGER.info("Received " + message + " for replication");
        try {
            KafkaReplicationWrapper kafkaReplicationWrapper = objectMapper.readValue(message, KafkaReplicationWrapper.class);
            String jpaMethod = kafkaReplicationWrapper.getOperation() + "WithoutReplicating";
            KafkaEntityWrapper kafkaEntityWrapper = kafkaReplicationWrapper.getKafkaEntityWrapper();
            Class<?> entityClass = Class.forName(kafkaEntityWrapper.getEntityClassName());
            Object entity = objectMapper.convertValue(kafkaEntityWrapper.getEntity(), entityClass);
            String repositoryName = entityClass.getSimpleName() + "Repository";
            ReplicatedJpaRepository replicatedJpaRepository = myEntitiesRepositories.stream().filter(repository ->
                    Arrays.stream(repository.getClass().getGenericInterfaces()).anyMatch(type ->
                            type.getTypeName().endsWith("." + repositoryName))).findAny()
                    .orElseThrow(() -> new ExceptionInInitializerError("Cannot find repository for class " + entityClass));
            Method repositoryMethod = ReplicatedJpaRepository.class.getMethod(jpaMethod, Object.class);
            repositoryMethod.invoke(replicatedJpaRepository, entity);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        } catch (IOException | InvocationTargetException e) {
            throw new IllegalArgumentException(message, e);
        }
    }
}
