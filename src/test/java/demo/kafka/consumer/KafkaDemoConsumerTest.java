package demo.kafka.consumer;

import demo.kafka.event.DemoInboundKey;
import demo.kafka.event.DemoInboundPayload;
import demo.kafka.service.DemoService;
import demo.kafka.util.TestEventData;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class KafkaDemoConsumerTest {

    private DemoService serviceMock;
    private KafkaDemoConsumer consumer;

    @BeforeEach
    public void setUp() {
        serviceMock = mock(DemoService.class);
        consumer = new KafkaDemoConsumer(serviceMock);
    }

    /**
     * Ensure that the JSON message is successfully passed on to the service.
     */
    @Test
    public void testListen_Success() {
        DemoInboundKey testKey = TestEventData.buildDemoInboundKey(RandomUtils.nextInt(1, 6));
        DemoInboundPayload testPayload = TestEventData.buildDemoInboundPayload(1);

        consumer.listen(0, testKey, testPayload);

        verify(serviceMock, times(1)).process(testKey, testPayload);
    }

    /**
     * If an exception is thrown, an error is logged but the processing completes successfully.
     *
     * This ensures the consumer offsets are updated so that the message is not redelivered.
     */
    @Test
    public void testListen_ServiceThrowsException() {
        DemoInboundKey testKey = TestEventData.buildDemoInboundKey(RandomUtils.nextInt(1, 6));
        DemoInboundPayload testPayload = TestEventData.buildDemoInboundPayload(1);

        doThrow(new RuntimeException("Service failure")).when(serviceMock).process(testKey, testPayload);

        consumer.listen(0, testKey, testPayload);

        verify(serviceMock, times(1)).process(testKey, testPayload);
    }
}
