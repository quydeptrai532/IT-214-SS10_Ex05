# BÀI TẬP 5 — ĐẢM BẢO THỨ TỰ SỰ KIỆN VỚI KAFKA PARTITION KEY

**Bối cảnh:** một đơn hàng phát ra 3 sự kiện `CREATED → PAID → SHIPPED` vào topic `order-tracking` (5 partitions).
**Ràng buộc:** các sự kiện của cùng một `orderId` bắt buộc phải xử lý đúng thứ tự. Xử lý `SHIPPED` trước `PAID` là lỗi nghiệp vụ nghiêm trọng.

---

## PHẦN 1 — PHÂN TÍCH

### 1.1. Vì sao chia Partition có thể làm mất thứ tự?

**Nguyên tắc gốc của Kafka:** Kafka **chỉ đảm bảo thứ tự trong phạm vi MỘT partition**. Giữa các partition **không có thứ tự nào** — chúng được ghi và đọc độc lập, song song.

Vậy thứ tự tổng thể bị phá vỡ khi:

1. **Producer không gắn key** → `key = null` → bộ chia mặc định dùng chiến lược *sticky/round-robin*, rải các record lần lượt sang các partition khác nhau. Ba sự kiện của cùng một đơn có thể rơi vào 3 partition khác nhau.
2. **Topic có nhiều partition + consumer group có nhiều consumer** → mỗi consumer được gán một tập partition riêng và **xử lý song song**. Không có cơ chế nào đồng bộ thứ tự giữa các consumer.
3. **Tốc độ xử lý khác nhau** → consumer ở partition A xử lý nhanh hơn consumer ở partition B, dẫn tới sự kiện "sau" (theo thời gian) lại được xử lý "trước".

### 1.2. Ví dụ cụ thể

Topic `order-tracking` có **5 partitions**. Ba sự kiện của đơn `O-100` được gửi **không kèm key**:

| Thứ tự gửi | Sự kiện | Partition nhận (round-robin) | Offset |
|---|---|---|---|
| 1 | `CREATED` | **P1** | 0 |
| 2 | `PAID` | **P3** | 0 |
| 3 | `SHIPPED` | **P2** | 0 |

Consumer group `order-tracking-group` có 3 consumer:

| Consumer | Được gán partition |
|---|---|
| C1 | P0, P1 |
| C2 | P2 |
| C3 | P3, P4 |

Diễn biến thực tế:

```
t=0ms   C1 đọc P1: CREATED  (O-100)  ✅
t=2ms   C2 đọc P2: SHIPPED  (O-100)  ❌  ← xử lý TRƯỚC PAID!
t=50ms  C3 đọc P3: PAID     (O-100)  ❌  ← đến sau SHIPPED
```

→ Hệ thống xử lý `SHIPPED` khi đơn còn chưa `PAID` ⇒ **lỗi nghiệp vụ**. Thứ tự **đúng theo thời gian gửi** nhưng **sai theo thời điểm xử lý**, vì 3 sự kiện nằm ở 3 partition không liên quan tới nhau.

### 1.3. Kết luận

> Muốn giữ thứ tự cho một thực thể (`orderId`), **tất cả sự kiện của thực thể đó phải nằm trong CÙNG MỘT partition**. Đó chính là việc dùng **Partition Key**.

---

## PHẦN 2 — THIẾT KẾ GIẢI PHÁP

### 2.1. Cơ chế Partition Key của Kafka

Khi producer gửi kèm key (`KafkaTemplate.send(topic, key, value)`), bộ chia mặc định (`DefaultPartitioner`) tính:

```
partition = murmur2( keyBytes ) % numPartitions
```

Vì `murmur2` là hàm **deterministic** (cùng input luôn cho cùng output), nên:

- **Cùng `orderId`** → luôn băm ra **cùng một partition** → mọi sự kiện `CREATED/PAID/SHIPPED` của đơn đó **xếp hàng tuần tự trong 1 partition**.
- Trong một partition, Kafka ghi theo offset tăng dần và **chỉ một consumer trong group** được gán partition đó ⇒ các sự kiện được xử lý **tuần tự, đúng thứ tự**.
- **Khác `orderId`** → có thể rơi vào partition khác nhau ⇒ **vẫn song song** giữa các đơn khác nhau ⇒ không mất hiệu năng.

### 2.2. Cách cấu hình Producer

```java
// key = orderId  ->  moi su kien cua cung 1 don luon vao dung 1 partition
kafkaTemplate.send("order-tracking", event.getOrderId(), jsonPayload);
```

Cấu hình producer cần bật để **thứ tự không bị phá ngay trong lúc gửi** (khi có retry):

| Thuộc tính | Giá trị | Lý do |
|---|---|---|
| `acks` | `all` | Đảm bảo ghi thành công trước khi gửi record kế tiếp |
| `enable.idempotence` | `true` | Chống ghi trùng khi retry, giữ đúng thứ tự |
| `max.in.flight.requests.per.connection` | `1` (hoặc `<=5` khi idempotence bật) | Nếu > 1 mà **không** bật idempotence, record retry có thể được ghi **sau** record gửi sau nó ⇒ đảo thứ tự |

### 2.3. Số lượng Consumer tối đa

> **Số consumer hiệu dụng tối đa = số partition = 5** (với topic `order-tracking` có 5 partitions).

- Mỗi partition chỉ được gán cho **đúng một consumer** trong cùng một group.
- Nhóm có 5 consumer → mỗi consumer giữ 1 partition → **song song tối đa**.
- Thêm consumer thứ 6 → **không còn partition nào để chia** ⇒ consumer đó **idle** (chỉ ngồi dự phòng khi có consumer khác chết) ⇒ **lãng phí tài nguyên**.
- Hệ quả kèm theo: dù có 5 consumer, mọi sự kiện của **một** đơn hàng vẫn chỉ do **một** consumer xử lý ⇒ **thứ tự vẫn được bảo toàn**.

---

## PHẦN 3 — TRIỂN KHAI

### 3.1. Topic (5 partitions)

```java
@Bean
public NewTopic orderTrackingTopic() {
    return TopicBuilder.name("order-tracking").partitions(5).replicas(1).build();
}
```

### 3.2. Producer — gắn key = orderId

```java
@Component
public class OrderEventProducer {

    public static final String TOPIC = "order-tracking";
    private final KafkaTemplate<String, String> kafkaTemplate;

    public void publish(OrderStatusEvent event) throws Exception {
        String json = objectMapper.writeValueAsString(event);
        // KEY CHINH LA orderId
        kafkaTemplate.send(TOPIC, event.getOrderId(), json)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Gui {} cua don {} -> partition={} offset={}",
                                event.getStatus(), event.getOrderId(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Gui that bai: {}", ex.getMessage());
                    }
                });
    }
}
```

### 3.3. Consumer — xử lý tuần tự + kiểm tra thứ tự

```java
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
    processed.put(event.getOrderId(), ...); // luu vet de kiem chung
}
```

Máy trạng thái hợp lệ: `null → CREATED → PAID → SHIPPED`.

### 3.4. File cấu hình `application.yml` (phần quan trọng)

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all                               # giu thu tu khi retry
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 1
    consumer:
      group-id: order-tracking-group
      auto-offset-reset: earliest
      enable-auto-commit: false
    listener:
      ack-mode: record
      concurrency: 5                          # = so partition -> toi da 5 consumer song song
```

---

## PHẦN 4 — KIỂM CHỨNG

Test `OrderTrackingOrderingTest` dùng **Embedded Kafka** (5 partitions) và kiểm tra 3 điều:

1. **Cùng partition:** tất cả sự kiện của một `orderId` có cùng `partition` (in ra bảng partition từng event).
2. **Đúng thứ tự:** consumer nhận đủ `CREATED → PAID → SHIPPED` theo đúng trình tự, **0 vi phạm**.
3. **Nhiều đơn vẫn song song:** các `orderId` khác nhau phân bổ sang các partition khác nhau.

Kết quả chạy thật được lưu ở `Ex05_TestEvidence.txt`.
