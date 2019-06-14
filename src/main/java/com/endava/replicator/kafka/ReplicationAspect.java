package com.endava.replicator.kafka;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

@Aspect
@Configuration
public class ReplicationAspect {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationAspect.class);

    private final KafkaReplicationSender kafkaReplicationSender;

    public ReplicationAspect(KafkaReplicationSender kafkaReplicationSender) {
        this.kafkaReplicationSender = kafkaReplicationSender;
    }

    @Around("execution(* com.endava.replicator.kafka.ReplicatedJpaRepository.save(..))")
    public void around(ProceedingJoinPoint joinPoint) {
        LOGGER.info("Begin intercepted replication event");
        Object savedEntity = joinPoint.getArgs()[0];
        kafkaReplicationSender.replicate(savedEntity);
        LOGGER.info("End intercepted replication event");
    }
}
