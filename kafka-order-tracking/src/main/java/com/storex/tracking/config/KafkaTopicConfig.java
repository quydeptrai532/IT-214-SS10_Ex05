package com.storex.tracking.config;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String TOPIC = "order-tracking";
    public static final int PARTITIONS = 5;

    @Bean
    public NewTopic orderTrackingTopic() {
        return TopicBuilder.name(TOPIC).partitions(PARTITIONS).replicas(1).build();
    }
}
