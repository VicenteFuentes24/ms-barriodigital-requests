package cl.barriodigital.barriodigitalrequests.kafka;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import org.slf4j.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@ConditionalOnProperty(name = "barriodigital.kafka.enabled", havingValue = "true")
public class KafkaOutboxWorker {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaOutboxWorker.class);
    private final KafkaOutboxStore store;
    private final CatalogNameResolver catalog;
    private final KafkaEventJson json;
    private final KafkaTemplate<String,String> kafka;
    private final Clock clock;
    private final KafkaPublisherProperties properties;
    public KafkaOutboxWorker(KafkaOutboxStore store, CatalogNameResolver catalog, KafkaEventJson json,
            KafkaTemplate<String,String> kafka, Clock clock, KafkaPublisherProperties properties) {
        this.store = store; this.catalog = catalog; this.json = json; this.kafka = kafka;
        this.clock = clock; this.properties = properties;
    }
    @Transactional(timeout = 90)
    public boolean prepareNext() {
        var next = store.next(false, clock.instant());
        if (next.isEmpty()) return false;
        var row = next.get();
        try {
            if (row.procedureId().length() > 100) throw new IllegalArgumentException("PROCEDURE_ID_EXCEEDS_REPORT_LIMIT");
            String name = row.procedureName() == null ? catalog.resolve(row.procedureId()) : row.procedureName();
            // This exact JSON is committed BEFORE any send, so retries cannot change its name/time/hash.
            store.prepared(row, name, json.encode(row.event(name)));
        } catch (CatalogNameResolver.NameUnavailableException | IllegalArgumentException failure) {
            retry(row, failure.getMessage());
        }
        return true;
    }
    @Transactional(timeout = 90)
    public boolean publishNext() {
        var next = store.next(true, clock.instant());
        if (next.isEmpty()) return false;
        var row = next.get();
        try {
            kafka.send(row.topic(), Long.toString(row.requestId()), row.payload()).get(properties.sendTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Exception failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            retry(row, "KAFKA_" + failure.getClass().getSimpleName());
            return true;
        }
        store.published(row, clock.instant());
        LOG.info("kafka_broker_acknowledged eventId={} requestId={} requestVersion={}", row.eventId(), row.requestId(), row.version());
        return true;
    }
    private void retry(KafkaOutboxStore.Row row, String reason) {
        store.retry(row, clock.instant().plusMillis(properties.retryDelay(row.attempts() + 1)), reason);
        LOG.warn("kafka_outbox_pending eventId={} requestId={} requestVersion={} attempt={} reason={}",
                row.eventId(), row.requestId(), row.version(), row.attempts() + 1, reason);
    }
}
