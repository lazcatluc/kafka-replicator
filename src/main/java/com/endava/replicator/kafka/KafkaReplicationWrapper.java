package com.endava.replicator.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class KafkaReplicationWrapper {
    private final String operation;
    private final KafkaEntityWrapper kafkaEntityWrapper;

    @JsonCreator
    public KafkaReplicationWrapper(@JsonProperty("operation") String operation,
                                   @JsonProperty("kafkaEntityWrapper") KafkaEntityWrapper kafkaEntityWrapper) {
        this.operation = operation;
        this.kafkaEntityWrapper = kafkaEntityWrapper;
    }

    public String getOperation() {
        return operation;
    }

    public KafkaEntityWrapper getKafkaEntityWrapper() {
        return kafkaEntityWrapper;
    }
}
