package com.endava.replicator.kafka;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaReplicationReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReplicationReceiver.class);

    private final ObjectMapper objectMapper;
    private final KafkaReplicationSuccessSender kafkaReplicationSuccessSender;
    private final KafkaReplicationFailureSender kafkaReplicationFailureSender;
    private final List<? extends ReplicatedJpaRepository<?, ?>> myEntitiesRepositories;

    public KafkaReplicationReceiver(ObjectMapper objectMapper,
                                    KafkaReplicationSuccessSender kafkaReplicationSuccessSender,
                                    KafkaReplicationFailureSender kafkaReplicationFailureSender,
                                    List<? extends ReplicatedJpaRepository<?, ?>> myEntitiesRepositories) {
        this.objectMapper = objectMapper;
        this.kafkaReplicationSuccessSender = kafkaReplicationSuccessSender;
        this.kafkaReplicationFailureSender = kafkaReplicationFailureSender;
        this.myEntitiesRepositories = myEntitiesRepositories;
    }

    @KafkaListener(topics = "${kafka.replication.topic}")
    public void listen(String message) throws IOException, InterruptedException, ExecutionException, TimeoutException {
        LOGGER.debug("Received " + message + " for replication");
        KafkaReplicationWrapper kafkaReplicationWrapper = objectMapper.readValue(message, KafkaReplicationWrapper.class);
        KafkaEntityWrapper kafkaEntityWrapper = kafkaReplicationWrapper.getKafkaEntityWrapper();
        try {
            String jpaMethod = kafkaReplicationWrapper.getOperation() + "WithoutReplicating";
            Class<?> entityClass = Class.forName(kafkaEntityWrapper.getEntityClassName());
            Object entity = objectMapper.convertValue(kafkaEntityWrapper.getEntity(), entityClass);
            String repositoryName = entityClass.getSimpleName() + "Repository";
            ReplicatedJpaRepository replicatedJpaRepository = myEntitiesRepositories.stream().filter(repository ->
                    Arrays.stream(repository.getClass().getGenericInterfaces()).anyMatch(type ->
                            type.getTypeName().endsWith("." + repositoryName))).findAny()
                    .orElseThrow(() -> new ClassNotFoundException("Cannot find repository for class " + entityClass));
            Method repositoryMethod = ReplicatedJpaRepository.class.getMethod(jpaMethod, Object.class);
            repositoryMethod.invoke(replicatedJpaRepository, entity);
            kafkaReplicationSuccessSender.sendReplicationSuccess(kafkaEntityWrapper);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            kafkaReplicationFailureSender.sendReplicationFailure(kafkaEntityWrapper, e);
            throw new ExceptionInInitializerError(e);
        } catch (InvocationTargetException e) {
            LOGGER.error("Error in replication", e);
            kafkaReplicationFailureSender.sendReplicationFailure(kafkaEntityWrapper, e);
        }
    }
}
