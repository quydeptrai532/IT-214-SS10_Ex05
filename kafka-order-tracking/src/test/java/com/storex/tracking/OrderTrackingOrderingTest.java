package com.storex.tracking;
import com.storex.tracking.consumer.OrderTrackingConsumer;
import com.storex.tracking.event.OrderStatusEvent;
import com.storex.tracking.producer.OrderEventProducer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 5, topics = {"order-tracking"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class OrderTrackingOrderingTest {

    @Autowired
    private OrderEventProducer producer;

    @Autowired
    private OrderTrackingConsumer consumer;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private Consumer<String, String> verificationConsumer() {
        Map<String, Object> props = KafkaTestUtils.consumerProps("verify-" + UUID.randomUUID(), "true", embeddedKafka);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Consumer<String, String> c = new DefaultKafkaConsumerFactory<String, String>(props).createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(c, "order-tracking");
        return c;
    }

    @Test
    void suKienCuaCungOrderIdVaoCungMotPartitionVaDungThuTu() throws Exception {
        String orderA = "O-100";
        String orderB = "O-200";
        List<String> flow = List.of("CREATED", "PAID", "SHIPPED");

        for (String status : flow) {
            producer.publish(new OrderStatusEvent(orderA, status, System.currentTimeMillis()));
        }
        for (String status : flow) {
            producer.publish(new OrderStatusEvent(orderB, status, System.currentTimeMillis()));
        }

        Consumer<String, String> verifier = verificationConsumer();
        Map<String, Set<Integer>> partitionsByKey = new HashMap<>();
        int collected = 0;
        long deadline = System.currentTimeMillis() + 40_000;
        while (System.currentTimeMillis() < deadline && collected < 6) {
            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(verifier, Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> r : records) {
                partitionsByKey.computeIfAbsent(r.key(), k -> new TreeSet<>()).add(r.partition());
                collected++;
            }
        }
        verifier.close();

        System.out.println(">>> Partition cua tung orderId: " + partitionsByKey);
        assertThat(partitionsByKey.get(orderA)).hasSize(1);
        assertThat(partitionsByKey.get(orderB)).hasSize(1);

        deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline && consumer.getProcessedStatuses(orderA).size() < 3) {
            Thread.sleep(200);
        }

        System.out.println(">>> Thu tu xu ly don " + orderA + ": " + consumer.getProcessedStatuses(orderA));
        System.out.println(">>> So vi pham thu tu: " + consumer.getViolationCount());

        assertThat(consumer.getProcessedStatuses(orderA)).containsExactly("CREATED", "PAID", "SHIPPED");
        assertThat(consumer.getViolationCount()).isZero();
    }
}
