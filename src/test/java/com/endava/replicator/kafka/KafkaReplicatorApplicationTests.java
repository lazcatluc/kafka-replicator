package com.endava.replicator.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@EmbeddedKafka(brokerProperties = {"listeners=PLAINTEXT://${kafka.bootstrapAddress}", "port=${kafka.port}"})
@Configuration
public class KafkaReplicatorApplicationTests {

    @Autowired
    private MyEntityRepository myEntityRepository;
    @Autowired
    private MyEntityService myEntityService;

    @Test
    public void receivesSentMessage() {
        MyEntity myEntity = new MyEntity();
        myEntityService.save(myEntity);
        Optional<MyEntity> byId = myEntityRepository.findById(myEntity.getId());
        assertThat(byId).isNotEmpty();
        myEntity = byId.get();
        myEntity.setDescription("new description");
        myEntityService.save(myEntity);
        byId = myEntityRepository.findById(myEntity.getId());
        assertThat(byId).isNotEmpty();
        myEntity = byId.get();
        assertThat(myEntity.getDescription()).isEqualTo("new description");
        myEntityRepository.delete(myEntity);
        assertThat(myEntityRepository.findById(myEntity.getId())).isEmpty();
    }

    @Test(expected = IllegalStateException.class)
    public void doesNotSendMessageWhenTransactionIsRolledBack() {
        MyEntity myEntity = new MyEntity();
        try {
            myEntityService.saveWithException(myEntity);
        } catch (IllegalStateException ise) {
            Optional<MyEntity> byId = myEntityRepository.findById(myEntity.getId());
            assertThat(byId).isEmpty();
            throw ise;
        }
    }
}
