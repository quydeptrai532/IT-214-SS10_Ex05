# BÀI TẬP 5 — ĐẢM BẢO THỨ TỰ SỰ KIỆN VỚI KAFKA PARTITION KEY

Xem phân tích đầy đủ tại `Ex05_Analysis.md`, bằng chứng chạy thật tại `Ex05_TestEvidence.txt`.

## Cấu trúc

```
Ex05/
├── Ex05_Analysis.md                  # Phần 1: vì sao partition làm mất thứ tự + cách khắc phục
├── Ex05_TestEvidence.txt             # Log chứng minh cùng orderId -> cùng partition -> đúng thứ tự
└── kafka-order-tracking/
    ├── build.gradle
    └── src/main/java/com/storex/tracking/
        ├── OrderTrackingApplication.java
        ├── config/KafkaTopicConfig.java      # topic order-tracking, 5 partitions
        ├── event/OrderStatusEvent.java
        ├── producer/OrderEventProducer.java  # ★ gui kem key = orderId
        ├── consumer/OrderTrackingConsumer.java # ★ xu ly tuan tu + kiem tra thu tu
        └── controller/TrackingController.java
```

## Điểm cốt lõi của lời giải

```java
// KEY chinh la orderId -> moi su kien cua cung 1 don luon vao cung 1 partition
kafkaTemplate.send("order-tracking", event.getOrderId(), jsonPayload);
```

Cấu hình producer để thứ tự không bị phá khi retry (`application.yml`):

```yaml
spring.kafka.producer:
  acks: all
  properties:
    enable.idempotence: true
    max.in.flight.requests.per.connection: 1
```

**Số consumer tối đa = số partition = 5.** Thêm consumer thứ 6 sẽ không có partition để nhận ⇒ idle, lãng phí.
Trong `application.yml`: `spring.kafka.listener.concurrency: 5`.

## Chạy test (không cần cài Kafka)

```bash
cd kafka-order-tracking
./gradlew test
```

Kết quả mong đợi: test `suKienCuaCungOrderIdVaoCungMotPartitionVaDungThuTu` PASS,
log cho thấy 3 sự kiện của `O-100` đều vào **partition 3** với offset 0 → 1 → 2,
và consumer xử lý đúng thứ tự `[CREATED, PAID, SHIPPED]` với **0 vi phạm**.

## Chạy thật với Kafka (nếu có broker ở localhost:9092)

```bash
cd kafka-order-tracking
./gradlew bootRun          # service ở port 8086
```

```bash
# Gui du 3 su kien CREATED -> PAID -> SHIPPED cho 1 don
curl -X POST http://localhost:8086/api/tracking/O-100/simulate

# Xem thu tu da xu ly
curl http://localhost:8086/api/tracking/O-100

# Xem so lan vi pham thu tu (mong doi = 0)
curl http://localhost:8086/api/tracking/violations
```

## Kết quả kiểm chứng

```
Gui CREATED cua don O-100 -> partition=3 offset=0
Gui PAID    cua don O-100 -> partition=3 offset=1
Gui SHIPPED cua don O-100 -> partition=3 offset=2
Gui CREATED cua don O-200 -> partition=4 offset=0     (don khac -> partition khac)
>>> Partition cua tung orderId: {O-200=[4], O-100=[3]}
>>> Thu tu xu ly don O-100: [CREATED, PAID, SHIPPED]
>>> So vi pham thu tu: 0
```
