package com.endava.replicator.kafka;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.NoRepositoryBean;

@NoRepositoryBean
public interface ReplicatedJpaRepository<T, ID> extends JpaRepository<T, ID> {

    default T saveWithoutReplicating(T readValue) {
        return save(readValue);
    }

    default void deleteWithoutReplicating(T entity) {
        delete(entity);
    }

    default void deleteByIdWithoutReplicating(ID id) {
        deleteById(id);
    }
}
