package com.endava.replicator.kafka;

import java.util.UUID;
import javax.persistence.Entity;
import javax.persistence.Id;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Entity
public class MyEntity {
    @Id
    private String id;
    private String description;

    public MyEntity() {
        id = UUID.randomUUID().toString();
    }

    @JsonCreator
    public MyEntity(@JsonProperty("id") String id, @JsonProperty("description") String description) {
        this.id = id;
        this.description = description;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "MyEntity{" + "id='" + id + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
