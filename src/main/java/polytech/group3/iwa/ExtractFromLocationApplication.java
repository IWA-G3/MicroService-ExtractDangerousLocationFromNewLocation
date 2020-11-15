package polytech.group3.iwa;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import polytech.group3.iwa.kafka_location_model.LocationKafka;

@SpringBootApplication
public class ExtractFromLocationApplication {

	public static void main(String[] args) {
		SpringApplication.run(ExtractFromLocationApplication.class, args);
	}

	@KafkaListener(topics = "location", groupId = "DangerLocationFromLocationListener")
	public void listen(LocationKafka message) {
		System.out.println("Received Messages in group - DangerLocationFromLocationListener: " + message);
	}

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public void sendMessage(String msg) {
		kafkaTemplate.send("dangerous_location", msg);
	}
}
