package cl.barriodigital.barriodigitalrequests.messaging;

import static cl.barriodigital.barriodigitalrequests.config.RequestsRabbitConfig.CERTIFICATE_KEY;
import static cl.barriodigital.barriodigitalrequests.config.RequestsRabbitConfig.COMMAND_EXCHANGE;
import static cl.barriodigital.barriodigitalrequests.config.RequestsRabbitConfig.CREW_KEY;
import static cl.barriodigital.barriodigitalrequests.config.RequestsRabbitConfig.EMAIL_KEY;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import cl.barriodigital.barriodigitalrequests.dto.RequestEvent;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
public class RequestEventPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestEventPublisher.class);

    private final RabbitTemplate rabbitTemplate;
    private final MeterRegistry meterRegistry;
    private final long confirmTimeoutMs;

    public RequestEventPublisher(RabbitTemplate rabbitTemplate, MeterRegistry meterRegistry,
            @Value("${barriodigital.rabbitmq.confirm-timeout-ms:5000}") long confirmTimeoutMs) {
        if (confirmTimeoutMs <= 0) {
            throw new IllegalArgumentException("RabbitMQ confirm timeout must be positive.");
        }
        this.rabbitTemplate = rabbitTemplate;
        this.meterRegistry = meterRegistry;
        this.confirmTimeoutMs = confirmTimeoutMs;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT, fallbackExecution = false)
    public void afterCommit(RequestEvent event) {
        List<String> keys = switch (event.status()) {
            case INGRESADO -> event.type() == RequestEvent.Type.REQUEST_CREATED
                    ? List.of(CERTIFICATE_KEY) : List.of();
            case ADMITIDO -> List.of(EMAIL_KEY);
            case EN_TERRENO -> List.of(CREW_KEY);
            case RESUELTO -> List.of(EMAIL_KEY, CERTIFICATE_KEY);
            default -> List.of();
        };
        for (String key : keys) {
            publish(event, key);
        }
    }

    private void publish(RequestEvent event, String key) {
        CorrelationData correlation = new CorrelationData(UUID.randomUUID().toString());
        try {
            rabbitTemplate.convertAndSend(COMMAND_EXCHANGE, key, event, message -> {
                var properties = message.getMessageProperties();
                properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                properties.setMessageId(event.eventId());
                properties.setTimestamp(Date.from(event.timestamp()));
                properties.setCorrelationId(event.correlationId());
                properties.setHeader("traceId", event.traceId());
                properties.getHeaders().remove("__TypeId__");
                return message;
            }, correlation);
            CorrelationData.Confirm confirm = correlation.getFuture().get(confirmTimeoutMs, TimeUnit.MILLISECONDS);
            if (correlation.getReturned() != null) {
                failed(event, key, "unroutable");
            } else if (!confirm.isAck()) {
                failed(event, key, "nack");
            } else {
                meterRegistry.counter("barriodigital.requests.events.published", "routing_key", key).increment();
            }
        } catch (AmqpException exception) {
            failed(event, key, "send_error");
        } catch (TimeoutException exception) {
            failed(event, key, "confirm_timeout");
        } catch (ExecutionException exception) {
            failed(event, key, "confirm_error");
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            failed(event, key, "interrupted");
        }
    }

    private void failed(RequestEvent event, String key, String reason) {
        meterRegistry.counter("barriodigital.requests.events.failed", "routing_key", key, "reason", reason).increment();
        LOGGER.error("RabbitMQ publication failed after DB commit: eventId={} requestId={} routingKey={} reason={}",
                event.eventId(), event.requestId(), key, reason);
    }
}
