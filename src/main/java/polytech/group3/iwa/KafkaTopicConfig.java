package polytech.group3.iwa;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
class KafkaTopicConfig {

    @Bean
    public NewTopic location() {
        return TopicBuilder.name("location").build();
    }

    @Bean
    public NewTopic dangerousLocation() {
        return TopicBuilder.name("dangerous_location").build();
    }

    @Bean
    public NewTopic contamination() {
        return TopicBuilder.name("contamination").build();
    }
}

