package com.endava.replicator.kafka;

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
}
