package com.storex.tracking.producer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.storex.tracking.config.KafkaTopicConfig;
import com.storex.tracking.event.OrderStatusEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventProducer {
    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public OrderEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(OrderStatusEvent event) throws Exception {
        String json = objectMapper.writeValueAsString(event);
        kafkaTemplate.send(KafkaTopicConfig.TOPIC, event.getOrderId(), json)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Gui that bai: {}", ex.getMessage());
                        return;
                    }
                    log.info("Gui {} cua don {} -> partition={} offset={}",
                            event.getStatus(), event.getOrderId(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                });
    }
}
