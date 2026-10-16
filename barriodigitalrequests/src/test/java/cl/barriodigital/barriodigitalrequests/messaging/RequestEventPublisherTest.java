package cl.barriodigital.barriodigitalrequests.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.net.ConnectException;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import cl.barriodigital.barriodigitalrequests.config.RequestsRabbitConfig;
import cl.barriodigital.barriodigitalrequests.dto.RequestEvent;
import cl.barriodigital.barriodigitalrequests.model.RequestStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

@ExtendWith(MockitoExtension.class)
class RequestEventPublisherTest {

    @Mock private RabbitTemplate rabbitTemplate;
    private SimpleMeterRegistry registry;
    private RequestEventPublisher publisher;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        publisher = new RequestEventPublisher(rabbitTemplate, registry, 10);
    }

    @Test
    void sendsPersistentJsonWithAmqpMetadataAndCountsOnlyAcknowledgedMessages() throws Exception {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        var converter = new RequestsRabbitConfig().requestEventMessageConverter(mapper);
        AtomicReference<Message> sent = new AtomicReference<>();
        RequestEvent event = event(RequestStatus.ADMITIDO);
        doAnswer(invocation -> {
            MessagePostProcessor processor = invocation.getArgument(3);
            sent.set(processor.postProcessMessage(converter.toMessage(invocation.getArgument(2), new MessageProperties())));
            CorrelationData correlation = invocation.getArgument(4);
            correlation.getFuture().complete(new CorrelationData.Confirm(true, null));
            return null;
        }).when(rabbitTemplate).convertAndSend(anyString(), anyString(), any(),
                any(MessagePostProcessor.class), any(CorrelationData.class));

        publisher.afterCommit(event);

        MessageProperties properties = sent.get().getMessageProperties();
        assertThat(properties.getDeliveryMode()).isEqualTo(MessageDeliveryMode.PERSISTENT);
        assertThat(properties.getContentType()).isEqualTo("application/json");
        assertThat(properties.getMessageId()).isEqualTo(event.eventId());
        assertThat(properties.getCorrelationId()).isEqualTo(event.correlationId());
        assertThat(properties.getHeaders().get("traceId")).isEqualTo(event.traceId());
        assertThat(properties.getHeaders()).doesNotContainKey("__TypeId__");
        assertThat(mapper.readTree(sent.get().getBody()).get("timestamp").asText()).isEqualTo(event.timestamp().toString());
        assertThat(mapper.readValue(sent.get().getBody(), RequestEvent.class)).isEqualTo(event);
        assertThat(registry.get("barriodigital.requests.events.published").tag("routing_key", "email.send").counter().count()).isEqualTo(1);
    }

    @Test
    void nackIsRecordedAsFailureWithoutEscapingTheAfterCommitListener() {
        completeWith(correlation -> correlation.getFuture().complete(new CorrelationData.Confirm(false, "nack")));
        publisher.afterCommit(event(RequestStatus.ADMITIDO));
        assertFailure("nack");
    }

    @Test
    void returnedMessageIsFailureEvenIfTheExchangeAcknowledgesIt() {
        completeWith(correlation -> {
            correlation.setReturned(new ReturnedMessage(new Message(new byte[0], new MessageProperties()),
                    312, "NO_ROUTE", "cmd.direct", "email.send"));
            correlation.getFuture().complete(new CorrelationData.Confirm(true, null));
        });
        publisher.afterCommit(event(RequestStatus.ADMITIDO));
        assertFailure("unroutable");
    }

    @Test
    void missingConfirmIsRecordedAsAnUncertainTimeout() {
        publisher.afterCommit(event(RequestStatus.ADMITIDO));
        assertFailure("confirm_timeout");
    }

    @Test
    void exceptionalConfirmIsRecordedAsFailure() {
        completeWith(correlation -> correlation.getFuture().completeExceptionally(new IllegalStateException("closed")));
        publisher.afterCommit(event(RequestStatus.ADMITIDO));
        assertFailure("confirm_error");
    }

    @Test
    void secondResolvedCommandIsAttemptedEvenIfTheEmailCommandFails() {
        doThrow(new AmqpConnectException(new ConnectException("offline"))).when(rabbitTemplate)
                .convertAndSend(eq("cmd.direct"), eq("email.send"), any(), any(MessagePostProcessor.class), any(CorrelationData.class));
        doAnswer(invocation -> {
            CorrelationData correlation = invocation.getArgument(4);
            correlation.getFuture().complete(new CorrelationData.Confirm(true, null));
            return null;
        }).when(rabbitTemplate).convertAndSend(eq("cmd.direct"), eq("certificate.gen"), any(),
                any(MessagePostProcessor.class), any(CorrelationData.class));
        publisher.afterCommit(event(RequestStatus.RESUELTO));
        assertFailure("send_error");
        verify(rabbitTemplate).convertAndSend(eq("cmd.direct"), eq("certificate.gen"), any(),
                any(MessagePostProcessor.class), any(CorrelationData.class));
        assertThat(registry.get("barriodigital.requests.events.published").tag("routing_key", "certificate.gen").counter().count()).isEqualTo(1);
    }

    private void completeWith(Consumer<CorrelationData> action) {
        doAnswer(invocation -> {
            action.accept(invocation.getArgument(4));
            return null;
        }).when(rabbitTemplate).convertAndSend(anyString(), anyString(), any(),
                any(MessagePostProcessor.class), any(CorrelationData.class));
    }

    private void assertFailure(String reason) {
        assertThat(registry.get("barriodigital.requests.events.failed")
                .tags("routing_key", "email.send", "reason", reason).counter().count()).isEqualTo(1);
        assertThat(registry.find("barriodigital.requests.events.published").tag("routing_key", "email.send").counter()).isNull();
    }

    private RequestEvent event(RequestStatus status) {
        return new RequestEvent(RequestEvent.Type.REQUEST_STATUS_CHANGED, "event-1", Instant.parse("2026-10-12T12:00:00Z"),
                "trace-1", "correlation-1", 1L, "procedure-1", status, "owner@example.invalid");
    }
}
