package com.storex.tracking.consumer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.storex.tracking.event.OrderStatusEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class OrderTrackingConsumer {
    private static final Logger log = LoggerFactory.getLogger(OrderTrackingConsumer.class);
    private static final List<String> ORDER = List.of("CREATED", "PAID", "SHIPPED");

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, String> lastStatus = new ConcurrentHashMap<>();
    private final Map<String, List<String>> processedStatuses = new ConcurrentHashMap<>();
    private final AtomicInteger violationCount = new AtomicInteger(0);

    @KafkaListener(topics = "order-tracking", groupId = "order-tracking-group")
    public void consume(ConsumerRecord<String, String> record) throws Exception {
        OrderStatusEvent event = objectMapper.readValue(record.value(), OrderStatusEvent.class);
        log.info("Nhan {} cua don {} | partition={} offset={} key={}",
                event.getStatus(), event.getOrderId(), record.partition(), record.offset(), record.key());

        String previous = lastStatus.get(event.getOrderId());
        if (!isValidTransition(previous, event.getStatus())) {
            violationCount.incrementAndGet();
            log.error("SAI THU TU! don {} : {} -> {}", event.getOrderId(), previous, event.getStatus());
        }
        lastStatus.put(event.getOrderId(), event.getStatus());
        processedStatuses.computeIfAbsent(event.getOrderId(), k -> Collections.synchronizedList(new ArrayList<>()))
                .add(event.getStatus());
    }

    private boolean isValidTransition(String previous, String current) {
        int currentIndex = ORDER.indexOf(current);
        if (currentIndex < 0) {
            return false;
        }
        if (previous == null) {
            return currentIndex == 0;
        }
        int previousIndex = ORDER.indexOf(previous);
        return currentIndex == previousIndex + 1;
    }

    public List<String> getProcessedStatuses(String orderId) {
        return processedStatuses.getOrDefault(orderId, List.of());
    }

    public int getViolationCount() {
        return violationCount.get();
    }
}
