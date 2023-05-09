package demo.kafka.util;

import demo.kafka.event.DemoInboundKey;
import demo.kafka.event.DemoInboundPayload;
import demo.kafka.event.DemoOutboundKey;
import demo.kafka.event.DemoOutboundPayload;

public class TestEventData {

    public static String INBOUND_DATA = "inbound event data";
    public static String OUTBOUND_DATA = "outbound event data";

    public static DemoInboundKey buildDemoInboundKey(Integer id) {
        return DemoInboundKey.builder()
                .id(id)
                .build();
    }

    public static DemoInboundPayload buildDemoInboundPayload(Integer sequenceNumber) {
        return DemoInboundPayload.builder()
                .inboundData(INBOUND_DATA)
                .sequenceNumber(sequenceNumber)
                .build();
    }

    public static DemoOutboundKey buildDemoOutboundKey(Integer id) {
        return DemoOutboundKey.builder()
                .id(id)
                .build();
    }

    public static DemoOutboundPayload buildDemoOutboundPayload(Integer sequenceNumber) {
        return DemoOutboundPayload.builder()
                .outboundData(OUTBOUND_DATA)
                .sequenceNumber(sequenceNumber)
                .build();
    }
}
