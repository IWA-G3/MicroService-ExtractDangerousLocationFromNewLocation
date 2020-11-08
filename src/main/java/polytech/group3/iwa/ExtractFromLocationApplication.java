package polytech.group3.iwa;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class ExtractFromLocationApplication {

	public static void main(String[] args) {
		SpringApplication.run(ExtractFromLocationApplication.class, args);
	}

	@KafkaListener(topics = "location", groupId = "DangerLocationFromLocationListener")
	public void listen(String message) {
		System.out.println("Received Messages in group - DangerLocationFromLocationListener: " + message);
	}
}
