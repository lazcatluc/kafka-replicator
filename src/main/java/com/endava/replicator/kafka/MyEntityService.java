package com.endava.replicator.kafka;

import java.util.List;
import javax.transaction.Transactional;
import org.springframework.stereotype.Service;

@Service
@Transactional
public class MyEntityService {
    private final MyEntityRepository myEntityRepository;

    public MyEntityService(MyEntityRepository myEntityRepository) {
        this.myEntityRepository = myEntityRepository;
    }

    public MyEntity save(MyEntity myEntity){
        return myEntityRepository.save(myEntity);
    }

    public MyEntity saveWithException(MyEntity myEntity) {
        myEntityRepository.save(myEntity);
        throw new IllegalStateException();
    }

    public List<MyEntity> saveAll(Iterable<MyEntity> myEntities) {
        return myEntityRepository.saveAll(myEntities);
    }

    public void deleteAll(Iterable<? extends MyEntity> iterable) {
        myEntityRepository.deleteAll(iterable);
    }
}
