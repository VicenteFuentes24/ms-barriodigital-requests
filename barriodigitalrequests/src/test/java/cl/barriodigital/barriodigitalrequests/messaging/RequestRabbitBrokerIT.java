package cl.barriodigital.barriodigitalrequests.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;

import cl.barriodigital.barriodigitalrequests.dto.CreateRequestRequest;
import cl.barriodigital.barriodigitalrequests.dto.RequestEvent;
import cl.barriodigital.barriodigitalrequests.model.RequestStatus;
import cl.barriodigital.barriodigitalrequests.service.RequestService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@EnabledIfEnvironmentVariable(named = "RABBITMQ_INTEGRATION_TEST", matches = "true")
@SpringBootTest(properties = {
        "spring.config.import=",
        "spring.datasource.url=jdbc:h2:mem:rabbit-live;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect"
})
class RequestRabbitBrokerIT {

    @Autowired private RequestService service;
    @Autowired private RabbitTemplate rabbitTemplate;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private MeterRegistry meterRegistry;

    @Test
    void committedCreationPublishesARealPersistentCertificateEvent() throws Exception {
        Integer queued = rabbitTemplate.execute(channel ->
                channel.queueDeclarePassive("q.cmd.certificate").getMessageCount());
        assertThat(queued).as("Use a dedicated broker with an empty certificate queue").isZero();
        var request = service.createRequest(new CreateRequestRequest("procedure-live-test", "Prueba AMQP aislada"), "owner@example.invalid");
        var message = rabbitTemplate.receive("q.cmd.certificate", 5000);
        assertThat(message).isNotNull();
        RequestEvent event = objectMapper.readValue(message.getBody(), RequestEvent.class);
        assertThat(event.type()).isEqualTo(RequestEvent.Type.REQUEST_CREATED);
        assertThat(event.requestId()).isEqualTo(request.id());
        assertThat(event.procedureTypeId()).isEqualTo(request.procedureTypeId());
        assertThat(event.status()).isEqualTo(RequestStatus.INGRESADO);
        assertThat(event.createdBy()).isEqualTo("owner@example.invalid");
        assertThat(message.getMessageProperties().getReceivedDeliveryMode()).isEqualTo(MessageDeliveryMode.PERSISTENT);
        assertThat(message.getMessageProperties().getContentType()).isEqualTo("application/json");
        assertThat(message.getMessageProperties().getReceivedRoutingKey()).isEqualTo("certificate.gen");
        assertThat(message.getMessageProperties().getMessageId()).isEqualTo(event.eventId());
        assertThat(message.getMessageProperties().getCorrelationId()).isEqualTo(event.correlationId());
        assertThat(meterRegistry.get("barriodigital.requests.events.published")
                .tag("routing_key", "certificate.gen").counter().count()).isEqualTo(1);
        System.out.println("Real RabbitMQ event received from q.cmd.certificate: " + new String(message.getBody(), StandardCharsets.UTF_8));
    }
}
