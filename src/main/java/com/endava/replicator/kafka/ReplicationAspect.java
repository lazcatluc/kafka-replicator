package com.endava.replicator.kafka;

import java.lang.reflect.Type;
import java.util.Arrays;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.context.annotation.Configuration;

@Aspect
@Configuration
@SuppressWarnings("unchecked")
public class ReplicationAspect {

    private final KafkaReplicationSender kafkaReplicationSender;

    public ReplicationAspect(KafkaReplicationSender kafkaReplicationSender) {
        this.kafkaReplicationSender = kafkaReplicationSender;
    }

    @Around("execution(* com.endava.replicator.kafka.ReplicatedJpaRepository.save(..))")
    public void aroundSave(ProceedingJoinPoint joinPoint) {
        Object savedEntity = joinPoint.getArgs()[0];
        kafkaReplicationSender.replicate("save", savedEntity);
    }

    @Around("execution(* com.endava.replicator.kafka.ReplicatedJpaRepository.delete(..))")
    public void aroundDelete(ProceedingJoinPoint joinPoint) {
        Object entity = joinPoint.getArgs()[0];
        kafkaReplicationSender.replicate("delete", entity);
    }

    @Around("execution(* com.endava.replicator.kafka.ReplicatedJpaRepository.deleteById(..))")
    public void aroundDeleteById(ProceedingJoinPoint joinPoint) {
        Class<?> targetClass = joinPoint.getTarget().getClass();
        Class<?> entityClass = Arrays.stream(targetClass.getGenericInterfaces()).map(Type::getTypeName)
                .filter(typeName -> {
                    try {
                        Class<?> aClass = Class.forName(typeName);
                        return ReplicatedJpaRepository.class.isAssignableFrom(aClass);
                    } catch (ClassNotFoundException e) {
                        throw new NoClassDefFoundError(typeName);
                    }
                })
                .findAny()
                .flatMap(typeName -> {
                    try {
                        return Arrays.stream(Class.forName(typeName).getGenericInterfaces())
                                .map(Type::getTypeName)
                                .filter(replicatedJpaRepositoryTypeName ->replicatedJpaRepositoryTypeName
                                        .startsWith(ReplicatedJpaRepository.class.getCanonicalName() + "<"))
                                .findAny()
                                .map(s -> s.split("<")[1].split(",")[0])
                                .map(s -> {
                                    try {
                                        return Class.forName(s);
                                    } catch (ClassNotFoundException e) {
                                        throw new NoClassDefFoundError(s);
                                    }
                                });
                    } catch (ClassNotFoundException e) {
                        throw new NoClassDefFoundError(typeName);
                    }
                }).orElseThrow(() ->
                        new NoClassDefFoundError("Cannot find ReplicatedJpaRepository interface for " + targetClass));
        Object entityId = joinPoint.getArgs()[0];
        kafkaReplicationSender.replicateById("delete", entityClass, entityId);
    }

    @Around("execution(* com.endava.replicator.kafka.ReplicatedJpaRepository.saveAll(..))")
    public void aroundSaveAll(ProceedingJoinPoint joinPoint) {
        ((Iterable) joinPoint.getArgs()[0]).forEach(savedEntity ->
                kafkaReplicationSender.replicate("save", savedEntity));
    }

    @Around("execution(* com.endava.replicator.kafka.ReplicatedJpaRepository.deleteAll(..))")
    public void aroundDeleteAll(ProceedingJoinPoint joinPoint) {
        ((Iterable) joinPoint.getArgs()[0]).forEach(entity ->
                kafkaReplicationSender.replicate("delete", entity));
    }
}
