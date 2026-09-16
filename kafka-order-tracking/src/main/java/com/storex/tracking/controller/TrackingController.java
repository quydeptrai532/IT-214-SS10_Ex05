package com.storex.tracking.controller;
import com.storex.tracking.consumer.OrderTrackingConsumer;
import com.storex.tracking.event.OrderStatusEvent;
import com.storex.tracking.producer.OrderEventProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;

@RestController
@RequestMapping("/api/tracking")
public class TrackingController {
    private final OrderEventProducer producer;
    private final OrderTrackingConsumer consumer;

    public TrackingController(OrderEventProducer producer, OrderTrackingConsumer consumer) {
        this.producer = producer;
        this.consumer = consumer;
    }

    @PostMapping("/{orderId}/events")
    public ResponseEntity<String> sendEvent(@PathVariable String orderId,
                                            @RequestParam String status) throws Exception {
        producer.publish(new OrderStatusEvent(orderId, status, System.currentTimeMillis()));
        return ResponseEntity.ok("Da gui " + status + " cho don " + orderId);
    }

    @PostMapping("/{orderId}/simulate")
    public ResponseEntity<String> simulate(@PathVariable String orderId) throws Exception {
        for (String status : List.of("CREATED", "PAID", "SHIPPED")) {
            producer.publish(new OrderStatusEvent(orderId, status, System.currentTimeMillis()));
            Thread.sleep(100);
        }
        return ResponseEntity.ok("Da gui du 3 su kien CREATED -> PAID -> SHIPPED cho don " + orderId);
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<List<String>> getProcessed(@PathVariable String orderId) {
        return ResponseEntity.ok(consumer.getProcessedStatuses(orderId));
    }

    @GetMapping("/violations")
    public ResponseEntity<Integer> getViolations() {
        return ResponseEntity.ok(consumer.getViolationCount());
    }
}
