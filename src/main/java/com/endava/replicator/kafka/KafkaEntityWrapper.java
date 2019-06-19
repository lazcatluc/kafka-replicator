package com.endava.replicator.kafka;

import java.util.Map;
import java.util.UUID;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class KafkaEntityWrapper {
    private final String id;
    private final String entityClassName;
    private final Map entity;

    public KafkaEntityWrapper(String entityClassName, Map entity) {
        this(UUID.randomUUID().toString(), entityClassName, entity);
    }

    @JsonCreator
    public KafkaEntityWrapper(@JsonProperty("id") String id,
                              @JsonProperty("entityClassName") String entityClassName,
                              @JsonProperty("entity") Map entity) {
        this.id = id;
        this.entityClassName = entityClassName;
        this.entity = entity;
    }

    public String getEntityClassName() {
        return entityClassName;
    }

    public Map getEntity() {
        return entity;
    }

    public String getId() {
        return id;
    }
}
