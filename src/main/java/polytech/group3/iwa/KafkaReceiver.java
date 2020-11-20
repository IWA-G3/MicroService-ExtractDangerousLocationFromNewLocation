package polytech.group3.iwa;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.style.ToStringCreator;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import polytech.group3.iwa.kafka_location_model.LocationKafka;
import polytech.group3.iwa.model.KafkaContamination;


import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CountDownLatch;

@Component
@EnableKafka
class KafkaReceiver {

    private List<KafkaContamination> contaminationList = new ArrayList<KafkaContamination>();
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReceiver.class);

    private CountDownLatch latch = new CountDownLatch(1);

    @Autowired
    private KafkaTemplate<String, LocationKafka> kafkaTemplate;

    public KafkaReceiver() {
        super();
    }

    public CountDownLatch getLatch() {
        return latch;
    }


    @KafkaListener(topics = "location")
    public void receive(LocationKafka location) {
        LOGGER.info("received location='{}'", location.toString());
        int i = contaminationList.size() - 1;
        List<Integer> listId = new ArrayList<Integer>();
        while(i >= 0 && (contaminationList.get(i).getUserid() != location.getUserid())) {
            i--;
        }
        if (contaminationList.get(i).getUserid() != location.getUserid()){
            kafkaTemplate.send("dangerous_location", String.valueOf(location.getUserid()), location);
        }
        latch.countDown();
    }

    @KafkaListener(
            groupId = "DangerLocationFromLocationListener",
            topicPartitions = @TopicPartition(
                    topic = "contamination",
                    partitionOffsets = { @PartitionOffset(
                            partition = "0",
                            initialOffset = "0") }))
    void listenToPartitionWithOffset(
            @Payload KafkaContamination message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) int offset) {
        LOGGER.info("Received contamination [{}] from partition-{} with offset-{}",
                message,
                partition,
                offset);

        contaminationList.add(message);
        /*
        int i = 0;

        while(i < contaminationList.size()) {
            contaminationList.remove(i);
        };
        */
        System.out.println("there are " + contaminationList.size() +  " contaminations");
        latch.countDown();
    }
}
