package com.endava.replicator.kafka;

import java.util.Map;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class KafkaEntityWrapper {
    private final String entityClassName;
    private final Map entity;

    @JsonCreator
    public KafkaEntityWrapper(@JsonProperty("entityClassName") String entityClassName,
                              @JsonProperty("entity") Map entity) {
        this.entityClassName = entityClassName;
        this.entity = entity;
    }

    public String getEntityClassName() {
        return entityClassName;
    }

    public Map getEntity() {
        return entity;
    }
}
