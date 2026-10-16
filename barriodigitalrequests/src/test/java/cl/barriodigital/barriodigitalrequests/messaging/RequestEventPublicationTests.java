package cl.barriodigital.barriodigitalrequests.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.net.ConnectException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import cl.barriodigital.barriodigitalrequests.dto.CreateRequestRequest;
import cl.barriodigital.barriodigitalrequests.dto.RequestEvent;
import cl.barriodigital.barriodigitalrequests.dto.UpdateRequestStatusRequest;
import cl.barriodigital.barriodigitalrequests.exception.InvalidStatusTransitionException;
import cl.barriodigital.barriodigitalrequests.model.RequestEntity;
import cl.barriodigital.barriodigitalrequests.model.RequestStatus;
import cl.barriodigital.barriodigitalrequests.repository.RequestRepository;
import cl.barriodigital.barriodigitalrequests.service.RequestService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest(properties = {
        "spring.config.import=",
        "spring.datasource.url=jdbc:h2:mem:request-events;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect"
})
@AutoConfigureMockMvc
class RequestEventPublicationTests {

    @Autowired private RequestService service;
    @Autowired private RequestRepository repository;
    @Autowired private PlatformTransactionManager transactionManager;
    @Autowired private ApplicationEventPublisher applicationEventPublisher;
    @Autowired private MockMvc mockMvc;

    @MockBean private RabbitTemplate rabbitTemplate;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
        doAnswer(invocation -> {
            CorrelationData correlation = invocation.getArgument(4);
            correlation.getFuture().complete(new CorrelationData.Confirm(true, null));
            return null;
        }).when(rabbitTemplate).convertAndSend(anyString(), anyString(), any(),
                any(MessagePostProcessor.class), any(CorrelationData.class));
        clearInvocations(rabbitTemplate);
    }

    @Test
    void creatingRequestPublishesCertificateOnlyAfterCommitWithCompleteSnapshot() {
        Instant before = Instant.now();
        var response = new TransactionTemplate(transactionManager).execute(transaction -> {
            var created = service.createRequest(new CreateRequestRequest("  procedure-1  ", "Descripcion"), "owner@example.invalid");
            verifyNoInteractions(rabbitTemplate);
            return created;
        });
        ArgumentCaptor<RequestEvent> captor = ArgumentCaptor.forClass(RequestEvent.class);
        verify(rabbitTemplate).convertAndSend(eq("cmd.direct"), eq("certificate.gen"), captor.capture(),
                any(MessagePostProcessor.class), any(CorrelationData.class));
        RequestEvent event = captor.getValue();
        assertThat(event.type()).isEqualTo(RequestEvent.Type.REQUEST_CREATED);
        assertThat(event.requestId()).isEqualTo(response.id());
        assertThat(event.procedureTypeId()).isEqualTo("procedure-1");
        assertThat(event.status()).isEqualTo(RequestStatus.INGRESADO);
        assertThat(event.createdBy()).isEqualTo("owner@example.invalid");
        assertThat(event.timestamp()).isBetween(before, Instant.now());
        assertThat(UUID.fromString(event.eventId())).isNotNull();
        assertThat(UUID.fromString(event.traceId())).isNotNull();
        assertThat(event.correlationId()).isEqualTo(event.traceId());
    }

    @Test
    void httpHeadersPropagateTracingWithoutChangingTheRestContract() throws Exception {
        mockMvc.perform(post("/api/requests")
                .header("X-User-Email", "owner@example.invalid")
                .header("X-Trace-Id", "trace-123")
                .header("X-Correlation-Id", "correlation-456")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"procedureTypeId\":\"1\",\"description\":\"Descripcion\"}"))
                .andExpect(status().isCreated());
        ArgumentCaptor<RequestEvent> captor = ArgumentCaptor.forClass(RequestEvent.class);
        verify(rabbitTemplate).convertAndSend(anyString(), anyString(), captor.capture(),
                any(MessagePostProcessor.class), any(CorrelationData.class));
        assertThat(captor.getValue().traceId()).isEqualTo("trace-123");
        assertThat(captor.getValue().correlationId()).isEqualTo("correlation-456");
    }

    @ParameterizedTest
    @EnumSource(value = RequestStatus.class, names = {"ADMITIDO", "EN_TERRENO", "RESUELTO"})
    void validTransitionsPublishExpectedCommands(RequestStatus next) {
        RequestStatus previous = switch (next) {
            case ADMITIDO -> RequestStatus.INGRESADO;
            case EN_TERRENO -> RequestStatus.EN_GESTION;
            default -> RequestStatus.EN_TERRENO;
        };
        Long id = seed(previous);
        service.updateStatus(id, new UpdateRequestStatusRequest(next, null));
        List<String> expectedKeys = switch (next) {
            case ADMITIDO -> List.of("email.send");
            case EN_TERRENO -> List.of("crew.ticket");
            default -> List.of("email.send", "certificate.gen");
        };
        ArgumentCaptor<String> keys = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<RequestEvent> events = ArgumentCaptor.forClass(RequestEvent.class);
        verify(rabbitTemplate, times(expectedKeys.size())).convertAndSend(eq("cmd.direct"), keys.capture(),
                events.capture(), any(MessagePostProcessor.class), any(CorrelationData.class));
        assertThat(keys.getAllValues()).containsExactlyElementsOf(expectedKeys);
        assertThat(events.getAllValues()).allSatisfy(event -> {
            assertThat(event.type()).isEqualTo(RequestEvent.Type.REQUEST_STATUS_CHANGED);
            assertThat(event.status()).isEqualTo(next);
            assertThat(event.requestId()).isEqualTo(id);
        });
        if (next == RequestStatus.RESUELTO) {
            assertThat(events.getAllValues().get(0)).isEqualTo(events.getAllValues().get(1));
        }
    }

    @ParameterizedTest
    @EnumSource(value = RequestStatus.class, names = {"EN_GESTION", "RECHAZADO"})
    void otherValidTransitionsDoNotPublishRabbitCommands(RequestStatus next) {
        service.updateStatus(seed(RequestStatus.ADMITIDO), new UpdateRequestStatusRequest(next, null));
        verifyNoInteractions(rabbitTemplate);
    }

    @Test
    void invalidTransitionDoesNotPublishAndDoesNotChangeTheRequest() {
        Long id = seed(RequestStatus.INGRESADO);
        assertThatThrownBy(() -> service.updateStatus(id, new UpdateRequestStatusRequest(RequestStatus.EN_TERRENO, null)))
                .isInstanceOf(InvalidStatusTransitionException.class);
        verifyNoInteractions(rabbitTemplate);
        assertThat(repository.findById(id).orElseThrow().getStatus()).isEqualTo(RequestStatus.INGRESADO);
    }

    @Test
    void rolledBackTransactionDiscardsTheRegisteredEvent() {
        new TransactionTemplate(transactionManager).executeWithoutResult(transaction -> {
            service.createRequest(new CreateRequestRequest("1", "Descripcion"), null);
            verifyNoInteractions(rabbitTemplate);
            transaction.setRollbackOnly();
        });
        verifyNoInteractions(rabbitTemplate);
        assertThat(repository.count()).isZero();
    }

    @Test
    void databaseFailureAfterRegisteringTheEventDoesNotPublish() {
        assertThatThrownBy(() -> new TransactionTemplate(transactionManager).executeWithoutResult(transaction -> {
            var response = service.createRequest(new CreateRequestRequest("1", "Descripcion"), null);
            repository.findById(response.id()).orElseThrow().setDescription(null);
            repository.flush();
        })).isInstanceOf(RuntimeException.class);
        verifyNoInteractions(rabbitTemplate);
        assertThat(repository.count()).isZero();
    }

    @Test
    void brokerFailureDoesNotTurnAnAlreadyCommittedHttpCreationIntoAnError() throws Exception {
        doThrow(new AmqpConnectException(new ConnectException("offline"))).when(rabbitTemplate)
                .convertAndSend(anyString(), anyString(), any(), any(MessagePostProcessor.class), any(CorrelationData.class));
        mockMvc.perform(post("/api/requests").contentType(MediaType.APPLICATION_JSON)
                .content("{\"procedureTypeId\":\"1\",\"description\":\"Descripcion\"}"))
                .andExpect(status().isCreated());
        assertThat(repository.count()).isEqualTo(1);
    }

    @Test
    void eventsPublishedOutsideATransactionDoNotSendMessages() {
        applicationEventPublisher.publishEvent(new RequestEvent(RequestEvent.Type.REQUEST_CREATED,
                UUID.randomUUID().toString(), Instant.now(), "trace", "correlation", 1L, "1", RequestStatus.INGRESADO, null));
        verifyNoInteractions(rabbitTemplate);
    }

    private Long seed(RequestStatus initialStatus) {
        RequestEntity entity = new RequestEntity();
        entity.setProcedureTypeId("1");
        entity.setDescription("Descripcion");
        entity.setStatus(initialStatus);
        return repository.save(entity).getId();
    }
}
