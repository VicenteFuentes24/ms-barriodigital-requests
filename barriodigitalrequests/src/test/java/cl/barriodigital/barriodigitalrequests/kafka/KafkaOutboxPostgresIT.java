package cl.barriodigital.barriodigitalrequests.kafka;

import cl.barriodigital.barriodigitalrequests.dto.*;
import cl.barriodigital.barriodigitalrequests.exception.InvalidStatusTransitionException;
import cl.barriodigital.barriodigitalrequests.model.*;
import cl.barriodigital.barriodigitalrequests.repository.RequestRepository;
import cl.barriodigital.barriodigitalrequests.service.RequestService;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.io.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.*;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

@SpringBootTest(properties={"spring.config.import=","barriodigital.kafka.enabled=true","barriodigital.kafka.worker-enabled=false",
        "spring.jpa.hibernate.ddl-auto=validate","spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.PostgreSQLDialect",
        "logging.level.root=WARN","logging.level.org.springframework=WARN","logging.level.org.hibernate=WARN","debug=false"})
class KafkaOutboxPostgresIT {
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.6-alpine")
            .withDatabaseName("requests_kafka_test")
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(256L*1024*1024));
    static {
        POSTGRES.start();
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(),POSTGRES.getUsername(),POSTGRES.getPassword())) {
            ScriptUtils.executeSqlScript(connection,new ClassPathResource("kafka/requests-existing.sql"));
            ScriptUtils.executeSqlScript(connection,new FileSystemResource("../sql/kafka/V001__requests_kafka_outbox.sql"));
        } catch (Exception e) { throw new ExceptionInInitializerError(e); }
    }
    @DynamicPropertySource static void database(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url",POSTGRES::getJdbcUrl);
        r.add("spring.datasource.username",POSTGRES::getUsername);
        r.add("spring.datasource.password",POSTGRES::getPassword);
    }
    @AfterAll static void stopTestDatabase() { POSTGRES.stop(); }
    @Autowired RequestService requests;
    @Autowired RequestRepository repository;
    @Autowired JdbcTemplate jdbc;
    @Autowired KafkaOutboxWorker worker;
    @Autowired KafkaOutboxStore store;
    @Autowired PlatformTransactionManager transactionManager;
    @MockBean RabbitTemplate rabbit;
    @MockBean KafkaTemplate<String,String> kafka;
    @MockBean CatalogNameResolver catalog;

    @BeforeEach void isolatedSetup() {
        // All resets are restricted to the container created above, never an environment-supplied DB.
        jdbc.execute("TRUNCATE bd_requests_kafka.history_hold,bd_requests_kafka.outbox,bd_requests_kafka.stream,public.requests RESTART IDENTITY");
        when(catalog.resolve(anyString())).thenReturn("Licencia municipal");
        when(kafka.send(anyString(),anyString(),anyString())).thenReturn(CompletableFuture.completedFuture(null));
        doAnswer(call -> { call.<CorrelationData>getArgument(4).getFuture().complete(new CorrelationData.Confirm(true,null)); return null; })
                .when(rabbit).convertAndSend(anyString(),anyString(),any(),any(MessagePostProcessor.class),any(CorrelationData.class));
        // Exclude Spring's SmartInitializingSingleton lifecycle callback from business assertions.
        clearInvocations(kafka, rabbit, catalog);
    }
    RequestResponse create() { return requests.createRequest(new CreateRequestRequest("12","Description"),"owner@example.invalid"); }
    void status(long id, RequestStatus next) { requests.updateStatus(id,new UpdateRequestStatusRequest(next,null)); }
    long count(String table) { return jdbc.queryForObject("SELECT count(*) FROM bd_requests_kafka."+table,Long.class); }
    void ready() { jdbc.update("UPDATE bd_requests_kafka.outbox SET available_at=?",Timestamp.from(Instant.EPOCH)); }

    @Test void creationAndAllTransitionsHaveConsecutiveVersionsAndUnchangedRabbitCommands() {
        var r = create();
        for (var state : List.of(RequestStatus.ADMITIDO,RequestStatus.EN_GESTION,RequestStatus.EN_TERRENO,RequestStatus.RESUELTO)) status(r.id(),state);
        assertThat(jdbc.queryForList("SELECT request_version FROM bd_requests_kafka.outbox ORDER BY request_version",Long.class))
                .containsExactly(1L,2L,3L,4L,5L);
        assertThat(jdbc.queryForList("SELECT previous_status FROM bd_requests_kafka.outbox ORDER BY request_version",String.class))
                .containsExactly(null,"INGRESADO","ADMITIDO","EN_GESTION","EN_TERRENO");
        verifyNoInteractions(catalog,kafka);
        verify(rabbit,times(5)).convertAndSend(eq("cmd.direct"),anyString(),any(),any(MessagePostProcessor.class),any(CorrelationData.class));
        for (int i=0;i<5;i++) { assertThat(worker.prepareNext()).isTrue(); assertThat(worker.publishNext()).isTrue(); }
        var body=ArgumentCaptor.forClass(String.class);
        verify(kafka,times(5)).send(eq("bd.requests.events.v1"),eq(r.id().toString()),body.capture());
        assertThat(body.getAllValues().getFirst()).contains("\"requestVersion\":1","\"previousStatus\":null","REQUEST_CREATED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM bd_requests_kafka.outbox WHERE published_at IS NOT NULL",Long.class)).isEqualTo(5);
        verify(catalog,times(1)).resolve("12");
    }
    @Test void rejectedTransitionDoesNotAllocateVersionOrEvent() {
        var r=create(); clearInvocations(rabbit);
        assertThatThrownBy(() -> status(r.id(),RequestStatus.RESUELTO)).isInstanceOf(InvalidStatusTransitionException.class);
        assertThat(count("outbox")).isEqualTo(1);
        assertThat(repository.findById(r.id()).orElseThrow().getStatus()).isEqualTo(RequestStatus.INGRESADO);
        verifyNoInteractions(rabbit);
    }
    @Test void rollbackLeavesNoRequestStreamOutboxOrRabbitPublication() {
        new TransactionTemplate(transactionManager).executeWithoutResult(tx -> { create(); tx.setRollbackOnly(); });
        assertThat(repository.count()).isZero(); assertThat(count("stream")).isZero(); assertThat(count("outbox")).isZero();
        verifyNoInteractions(rabbit);
    }
    @Test void failingOutboxInsertRollsBackTheCoreWrite() {
        jdbc.execute("ALTER TABLE bd_requests_kafka.outbox ADD CONSTRAINT test_reject CHECK(request_version < 0)");
        try {
            assertThatThrownBy(this::create).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
            assertThat(repository.count()).isZero(); assertThat(count("stream")).isZero(); assertThat(count("outbox")).isZero();
            verifyNoInteractions(rabbit);
        } finally { jdbc.execute("ALTER TABLE bd_requests_kafka.outbox DROP CONSTRAINT test_reject"); }
    }
    @Test void catalogFailureDoesNotBlockCoreOrRabbitAndIsRecoverable() {
        when(catalog.resolve("12")).thenThrow(new CatalogNameResolver.NameUnavailableException("CATALOG_UNAVAILABLE"));
        var r=create(); assertThat(worker.prepareNext()).isTrue(); assertThat(worker.publishNext()).isFalse();
        assertThat(repository.existsById(r.id())).isTrue(); assertThat(count("outbox")).isEqualTo(1);
        verifyNoInteractions(kafka);
        verify(rabbit).convertAndSend(eq("cmd.direct"),eq("certificate.gen"),any(),any(MessagePostProcessor.class),any(CorrelationData.class));
        doReturn("Licencia municipal").when(catalog).resolve("12"); ready();
        assertThat(worker.prepareNext()).isTrue(); assertThat(worker.publishNext()).isTrue();
    }
    @Test void kafkaFailurePreservesFrozenPayloadAndBlocksLaterVersion() {
        var r=create(); status(r.id(),RequestStatus.ADMITIDO); worker.prepareNext();
        String payload=jdbc.queryForObject("SELECT payload FROM bd_requests_kafka.outbox WHERE request_version=1",String.class);
        when(kafka.send(anyString(),anyString(),anyString())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("offline")));
        worker.publishNext(); assertThat(worker.prepareNext()).isFalse(); assertThat(worker.publishNext()).isFalse();
        when(catalog.resolve("12")).thenReturn("Renamed later"); ready();
        when(kafka.send(anyString(),anyString(),anyString())).thenReturn(CompletableFuture.completedFuture(null));
        worker.publishNext();
        verify(kafka,times(2)).send(eq("bd.requests.events.v1"),eq(r.id().toString()),eq(payload));
        worker.prepareNext(); worker.publishNext();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM bd_requests_kafka.outbox WHERE published_at IS NOT NULL",Long.class)).isEqualTo(2);
    }
    @Test void legacyChangesAreDurablyHeldWithoutInventingCreationHistory() {
        var old=new RequestEntity(); old.setProcedureTypeId("12"); old.setDescription("Existing record"); old.setStatus(RequestStatus.ADMITIDO);
        long id=repository.save(old).getId();
        status(id,RequestStatus.EN_GESTION); status(id,RequestStatus.EN_TERRENO);
        assertThat(count("history_hold")).isEqualTo(2); assertThat(count("outbox")).isZero();
        assertThat(worker.prepareNext()).isFalse(); assertThat(worker.publishNext()).isFalse();
        assertThat(jdbc.queryForObject("SELECT tracking_status FROM bd_requests_kafka.stream WHERE request_id=?",String.class,id)).isEqualTo("BLOCKED_HISTORY");
    }
    @Test void detectsChangesMadeWhileCaptureWasDisabled() {
        var r=create();
        jdbc.update("UPDATE public.requests SET status='ADMITIDO' WHERE id=?",r.id());
        status(r.id(),RequestStatus.EN_GESTION);
        assertThat(count("history_hold")).isEqualTo(1); assertThat(worker.prepareNext()).isFalse();
    }
    @Test void concurrentSameTransitionHasExactlyOneWinnerAndNoVersionGap() throws Exception {
        var r=create(); var gate=new CountDownLatch(1);
        try(var pool=Executors.newFixedThreadPool(2)) {
            Callable<Boolean> task=() -> { gate.await(); try { status(r.id(),RequestStatus.ADMITIDO); return true; }
                catch(InvalidStatusTransitionException e) { return false; } };
            var a=pool.submit(task); var b=pool.submit(task); gate.countDown();
            assertThat(List.of(a.get(20,TimeUnit.SECONDS),b.get(20,TimeUnit.SECONDS))).containsExactlyInAnyOrder(true,false);
        }
        assertThat(jdbc.queryForList("SELECT request_version FROM bd_requests_kafka.outbox ORDER BY request_version",Long.class)).containsExactly(1L,2L);
    }
    @Test void anotherWorkerCannotPassLockedFirstEvent() throws Exception {
        var r=create(); status(r.id(),RequestStatus.ADMITIDO); worker.prepareNext();
        var locked=new CountDownLatch(1); var release=new CountDownLatch(1);
        try(var pool=Executors.newSingleThreadExecutor()) {
            var holder=pool.submit(() -> new TransactionTemplate(transactionManager).execute(tx -> {
                var row=store.next(true,Instant.now()); assertThat(row).isPresent(); locked.countDown();
                try { if(!release.await(10,TimeUnit.SECONDS)) throw new IllegalStateException("timeout"); }
                catch(InterruptedException e) { throw new RuntimeException(e); } return null;
            }));
            try { assertThat(locked.await(10,TimeUnit.SECONDS)).isTrue(); assertThat(worker.publishNext()).isFalse(); assertThat(worker.prepareNext()).isFalse(); }
            finally { release.countDown(); }
            holder.get(15,TimeUnit.SECONDS);
        }
        assertThat(worker.publishNext()).isTrue();
    }
    @Test void replayAfterSendButRolledBackMarkerUsesExactlyTheSameJson() {
        var r=create(); worker.prepareNext();
        new TransactionTemplate(transactionManager).executeWithoutResult(tx -> { worker.publishNext(); tx.setRollbackOnly(); });
        assertThat(worker.publishNext()).isTrue();
        var body=ArgumentCaptor.forClass(String.class);
        verify(kafka,times(2)).send(eq("bd.requests.events.v1"),eq(r.id().toString()),body.capture());
        assertThat(body.getAllValues().get(0)).isEqualTo(body.getAllValues().get(1));
    }
    @Test void requestsLongProcedureIdsRemainValidButCannotSendInvalidReportJson() {
        var r=requests.createRequest(new CreateRequestRequest("a".repeat(110),"Description"),null);
        assertThat(repository.existsById(r.id())).isTrue(); worker.prepareNext();
        assertThat(worker.publishNext()).isFalse(); verifyNoInteractions(kafka,catalog);
        assertThat(jdbc.queryForObject("SELECT last_error FROM bd_requests_kafka.outbox",String.class)).isEqualTo("PROCEDURE_ID_EXCEEDS_REPORT_LIMIT");
    }
}
