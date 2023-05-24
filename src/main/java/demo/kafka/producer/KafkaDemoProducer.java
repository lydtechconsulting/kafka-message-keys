package demo.kafka.producer;

import demo.kafka.event.DemoOutboundKey;
import demo.kafka.event.DemoOutboundPayload;
import demo.kafka.properties.KafkaDemoProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaDemoProducer {
    @Autowired
    private final KafkaDemoProperties properties;

    @Autowired
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    /**
     * Sends events asynchronously.
     */
    public void sendMessage(DemoOutboundKey key, DemoOutboundPayload event) {
        try {
            kafkaTemplate.send(properties.getOutboundTopic(), key, event);
            log.info("Emitted message - key: " + key + " - payload: " + event.getOutboundData());
        } catch (Exception e) {
            String message = "Error sending message to topic " + properties.getOutboundTopic();
            log.error(message);
            throw new RuntimeException(message, e);
        }
    }
}
