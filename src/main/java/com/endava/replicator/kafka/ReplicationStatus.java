package com.endava.replicator.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ReplicationStatus {
    private final String host;
    private final String message;

    @JsonCreator
    public ReplicationStatus(@JsonProperty("host") String host, @JsonProperty("message") String message) {
        this.host = host;
        this.message = message;
    }

    public String getHost() {
        return host;
    }

    public String getMessage() {
        return message;
    }
}
