package demo.kafka.producer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import demo.kafka.event.DemoOutboundKey;
import demo.kafka.event.DemoOutboundPayload;
import demo.kafka.properties.KafkaDemoProperties;
import demo.kafka.util.TestEventData;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaDemoProducerTest {

    private KafkaDemoProperties propertiesMock;
    private KafkaTemplate kafkaTemplateMock;
    private KafkaDemoProducer kafkaDemoProducer;

    @BeforeEach
    public void setUp() {
        propertiesMock = mock(KafkaDemoProperties.class);
        kafkaTemplateMock = mock(KafkaTemplate.class);
        kafkaDemoProducer = new KafkaDemoProducer(propertiesMock, kafkaTemplateMock);
    }

    /**
     * Ensure the Kafka client is called to emit a message.
     */
    @Test
    public void testSendMessage_Success() throws Exception {
        DemoOutboundKey testKey = TestEventData.buildDemoOutboundKey(RandomUtils.nextInt(1, 6));
        DemoOutboundPayload testPayload = TestEventData.buildDemoOutboundPayload(1);
        String topic = "test-outbound-topic";

        when(propertiesMock.getOutboundTopic()).thenReturn(topic);
        CompletableFuture futureResult = mock(CompletableFuture.class);
        SendResult expectedSendResult = mock(SendResult.class);
        when(futureResult.get()).thenReturn(expectedSendResult);
        when(kafkaTemplateMock.send(topic, testKey, testPayload)).thenReturn(futureResult);

        SendResult result = kafkaDemoProducer.sendMessage(testKey, testPayload);

        verify(kafkaTemplateMock, times(1)).send(topic, testKey, testPayload);
        assertThat(result, equalTo(expectedSendResult));
    }

    /**
     * Ensure that an exception thrown on the send is cleanly handled.
     */
    @Test
    public void testSendMessage_ExceptionOnSend() throws Exception {
        DemoOutboundKey testKey = TestEventData.buildDemoOutboundKey(RandomUtils.nextInt(1, 6));
        DemoOutboundPayload testPayload = TestEventData.buildDemoOutboundPayload(1);
        String topic = "test-outbound-topic";

        when(propertiesMock.getOutboundTopic()).thenReturn(topic);
        CompletableFuture futureResult = mock(CompletableFuture.class);
        when(kafkaTemplateMock.send(topic, testKey, testPayload)).thenReturn(futureResult);

        doThrow(new ExecutionException("Kafka send failure", new Exception("Failed"))).when(futureResult).get();

        Exception exception = assertThrows(RuntimeException.class, () -> {
                    kafkaDemoProducer.sendMessage(testKey, testPayload);
        });

        verify(kafkaTemplateMock, times(1)).send(topic, testKey, testPayload);
        assertThat(exception.getMessage(), equalTo("Error sending message to topic " + topic));
    }
}
