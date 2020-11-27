package polytech.group3.iwa;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
import polytech.group3.iwa.model.ContaminationKafka;


import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CountDownLatch;

@Component
@EnableKafka
class KafkaReceiver {

    private List<ContaminationKafka> contaminationList = new ArrayList<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReceiver.class);

    private CountDownLatch latch = new CountDownLatch(1);

    public KafkaReceiver() {
        super();
    }

    public CountDownLatch getLatch() {
        return latch;
    }


    @KafkaListener(
            groupId = "location",
            containerFactory = "kafkaListenerContainerFactoryLocation",
            topics="location")
    public void receive(@Payload LocationKafka location,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                        @Header(KafkaHeaders.OFFSET) int offset) {
        LOGGER.info("received location='{}'", location.toString());
        KafkaProducerConfig kafkaSender = new KafkaProducerConfig();
        int i = 0;
        //Send location to dangerous location topic if the user is contaminated
        System.out.println(contaminationList.size());
        while (i < contaminationList.size()) {
            if (contaminationList.get(i).getId_keycloak().equals(location.getUserid()) && Duration.between(contaminationList.get(i).getReporting_date(), LocalDateTime.now()).toHours() < 168) {
                kafkaSender.sendMessage(location,  "dangerous_location");
                break;
            }
        }
        latch.countDown();
    }

    @KafkaListener(
            groupId = "contamination",
            topicPartitions = @TopicPartition(
                    topic = "covid_alert_db.public.covid_info",
                    partitionOffsets = { @PartitionOffset(
                            partition = "0",
                            initialOffset = "0") }))
    public void listenToPartitionWithOffset(
            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) int offset) throws IOException {
        // Parse JSON object to retrieve id_keycloak
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        JsonParser parser = factory.createParser(key);
        JsonNode payload = mapper.readTree(parser);
        String idKeycloak = mapper.writeValueAsString(payload.get("payload").get("id_keycloak")).replace("\"","");
        String id_case_type =  mapper.writeValueAsString(payload.get("payload").get("id_case_type")).replace("\"", "");
        String reporting_date =  mapper.writeValueAsString(payload.get("payload").get("reporting_date")).replace("\"", "");
        LocalDateTime dateReporting = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(reporting_date)/1000), TimeZone.getDefault().toZoneId());
        LOGGER.info("received new contamination for user='{}'", idKeycloak);
        LOGGER.info("received contamination type = '{}'", id_case_type);
        LOGGER.info("received contamination date ='{}'", dateReporting);
        KafkaProducerConfig kafkaSender = new KafkaProducerConfig();

        if (Integer.parseInt(id_case_type) ==  1 && Duration.between(dateReporting, LocalDateTime.now()).toHours() < 168) {
            contaminationList.add(new ContaminationKafka(idKeycloak, Integer.parseInt(id_case_type), dateReporting));
        }

        latch.countDown();
    }
}
