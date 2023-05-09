package demo.kafka.consumer;

import demo.kafka.event.DemoInboundKey;
import demo.kafka.event.DemoInboundPayload;
import demo.kafka.service.DemoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class KafkaDemoConsumer {

    final DemoService demoService;

    @KafkaListener(
            topics = "demo-inbound-topic",
            groupId = "demo-consumer-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void listen(@Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition, @Header(KafkaHeaders.RECEIVED_KEY) DemoInboundKey key, @Payload final DemoInboundPayload payload) {
        log.info("Received message - key id: " + key.getId() + " - partition: " + partition + " - payload: " + payload);
        try {
            demoService.process(key, payload);
        } catch (Exception e) {
            log.error("Error processing message: " + e.getMessage());
        }
    }
}
