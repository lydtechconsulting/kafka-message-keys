package demo.kafka.integration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import demo.kafka.KafkaDemoConfiguration;
import demo.kafka.event.DemoInboundKey;
import demo.kafka.event.DemoInboundPayload;
import demo.kafka.event.DemoOutboundKey;
import demo.kafka.event.DemoOutboundPayload;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import static demo.kafka.util.TestEventData.buildDemoInboundKey;
import static demo.kafka.util.TestEventData.buildDemoInboundPayload;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

@Slf4j
@SpringBootTest(classes = { KafkaDemoConfiguration.class } )
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true, topics = { "demo-inbound-topic", "demo-outbound-topic" }, partitions = 10)
public class KafkaIntegrationTest {

    final static String DEMO_INBOUND_TEST_TOPIC = "demo-inbound-topic";
    final static String DEMO_OUTBOUND_TEST_TOPIC = "demo-outbound-topic";

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private KafkaTestListener testReceiver;

    @Configuration
    static class TestConfig {

        /**
         * The test listener.
         */
        @Bean
        public KafkaTestListener testReceiver() {
            return new KafkaTestListener();
        }
    }

    /**
     * Use this receiver to consume messages from the outbound topic.
     */
    public static class KafkaTestListener {
        AtomicInteger counter = new AtomicInteger(0);

        // Track the messages received, as a list of key/value pairs.
        List<ImmutablePair<DemoOutboundKey, DemoOutboundPayload>> keyedMessages = new ArrayList<>();

        // Track the partitions that each event key is received on.
        Map<DemoOutboundKey, Set<Integer>> partitionsByKey = new HashMap<>();

        // Key to Partition / SeqNo
        Map<DemoOutboundKey, List<ImmutablePair<Integer, Integer>>> messagesByKey = new HashMap<>();

        @KafkaListener(groupId = "KafkaIntegrationTest", topics = DEMO_OUTBOUND_TEST_TOPIC, autoStartup = "true")
        void receive(@Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition, @Header(KafkaHeaders.RECEIVED_KEY) DemoOutboundKey key, @Payload final DemoOutboundPayload payload) {
            log.debug("KafkaTestListener - Received message: partition: " + partition + " - key: " + key.getId() + " - outbound data: " + payload.getOutboundData());
            assertThat(key, notNullValue());
            assertThat(payload, notNullValue());
            keyedMessages.add(ImmutablePair.of(key, payload));

            Set<Integer> partitions = partitionsByKey.get(key);
            if(partitions==null) {
                partitions = new HashSet<>();
                partitionsByKey.put(key, partitions);
            }
            partitions.add(partition);

            List<ImmutablePair<Integer, Integer>> messages;
            if(!messagesByKey.containsKey(key)) {
                messages = new ArrayList<>();
                messagesByKey.put(key, messages);
            } else {
                messages = messagesByKey.get(key);
            }
            messages.add(ImmutablePair.of(partition, payload.getSequenceNumber()));


            counter.incrementAndGet();
        }
    }

    @BeforeEach
    public void setUp() {
        // Wait until the partitions are assigned.
        registry.getListenerContainers().stream().forEach(container ->
                ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic()));
        testReceiver.counter.set(0);
        testReceiver.keyedMessages = new ArrayList<>();
        testReceiver.partitionsByKey = new HashMap<>();
        testReceiver.messagesByKey = new HashMap<>();
    }

    /**
     * Send in a single event and ensure an outbound event is emitted.  Assert the outbound event is as expected.
     *
     * The key id sent with the inbound event should match the key id received for the outbound event.
     */
    @Test
    public void testSingleEvent() throws Exception {
        Integer keyId = RandomUtils.nextInt(1, 6);
        DemoInboundKey inboundKey = buildDemoInboundKey(keyId);

        DemoInboundPayload inboundPayload = buildDemoInboundPayload(1);
        kafkaTemplate.send(DEMO_INBOUND_TEST_TOPIC, inboundKey, inboundPayload).get();

        Awaitility.await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiver.counter::get, equalTo(1));

        assertThat(testReceiver.keyedMessages.size(), equalTo(1));
        assertThat(testReceiver.keyedMessages.get(0).getLeft().getId(), equalTo(keyId));
        assertThat(testReceiver.keyedMessages.get(0).getRight().getOutboundData(), equalTo("Processed: " + inboundPayload.getInboundData()));
    }

    /**
     * Send in multiple events asynchronously and ensure an outbound event is emitted for each.
     *
     * Verify that each event key is only ever received on the same partition.
     *
     * Each event has an ascending sequence number, so verify the sequence number of each event received is always ascending.
     *
     * Sending 10000 messages, with 5 unique keys, to 10 partitions.
     */
    @Test
    public void testKeyOrdering() throws Exception {
        int totalMessages = 10000;
        for (int i=0; i<totalMessages; i++) {
            Integer keyId = RandomUtils.nextInt(1, 6);
            DemoInboundKey inboundKey = buildDemoInboundKey(keyId);
            DemoInboundPayload inboundPayload = buildDemoInboundPayload(i+1);
            kafkaTemplate.send(DEMO_INBOUND_TEST_TOPIC, inboundKey, inboundPayload);
        }

        Awaitility.await().atMost(60, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiver.counter::get, equalTo(totalMessages));

        // Each event key should only ever be present on one partition.
        testReceiver.partitionsByKey.entrySet().stream().forEach(map -> assertThat(map.getValue().size(), equalTo(1)));

        testReceiver.messagesByKey.keySet().stream().forEach(key -> {
            List<ImmutablePair<Integer, Integer>> messages = testReceiver.messagesByKey.get(key);

            // All messages for this key should be on the same partition.
            assertThat(messages.stream().map(map -> (map.getLeft())).distinct().count(), equalTo(1L));

            // All messages for this key should have an ascending sequenceNumber.
            messages.stream()
                .map(map -> (map.getRight()))
                .reduce((sequenceCurrent, sequenceNext) -> {
                    log.debug("sequenceCurrent: {} - sequenceNext: {}", sequenceCurrent, sequenceNext);
                    assertThat(sequenceCurrent, lessThan(sequenceNext));
                    return sequenceNext;
                });
        });
    }
}
