package com.storex.tracking;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class OrderTrackingApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderTrackingApplication.class, args);
    }
}
