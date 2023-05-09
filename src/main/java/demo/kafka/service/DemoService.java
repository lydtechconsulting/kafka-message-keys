package demo.kafka.service;

import demo.kafka.event.DemoInboundKey;
import demo.kafka.event.DemoInboundPayload;
import demo.kafka.event.DemoOutboundKey;
import demo.kafka.event.DemoOutboundPayload;
import demo.kafka.producer.KafkaDemoProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class DemoService {

    @Autowired
    private final KafkaDemoProducer kafkaDemoProducer;

    public void process(DemoInboundKey key, DemoInboundPayload payload) {
        DemoOutboundPayload outboundPayload = DemoOutboundPayload.builder()
                .outboundData("Processed: " + payload.getInboundData())
                .sequenceNumber(payload.getSequenceNumber())
                .build();
        // Set the id of the outbound key to the id of the inbound key.
        DemoOutboundKey outboundKey = DemoOutboundKey.builder()
                .id(key.getId())
                .build();
        kafkaDemoProducer.sendMessage(outboundKey, outboundPayload);
    }
}
