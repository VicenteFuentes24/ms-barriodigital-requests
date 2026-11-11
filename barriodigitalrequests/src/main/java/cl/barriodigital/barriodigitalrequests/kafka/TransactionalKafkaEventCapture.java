package cl.barriodigital.barriodigitalrequests.kafka;

import cl.barriodigital.barriodigitalrequests.model.*;
import java.sql.Timestamp;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.*;

@Service
@ConditionalOnProperty(name = "barriodigital.kafka.enabled", havingValue = "true")
@Transactional(propagation = Propagation.MANDATORY)
public class TransactionalKafkaEventCapture implements KafkaEventCapture {
    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final KafkaPublisherProperties properties;
    public TransactionalKafkaEventCapture(JdbcTemplate jdbc, Clock clock, KafkaPublisherProperties properties) {
        this.jdbc = jdbc; this.clock = clock; this.properties = properties;
    }
    public boolean enabled() { return true; }
    public void created(RequestEntity request) {
        Instant time = request.getCreatedAt().truncatedTo(ChronoUnit.MICROS);
        jdbc.update("""
            INSERT INTO bd_requests_kafka.stream(request_id,tracking_status,last_version,last_status,last_occurred_at,procedure_id)
            VALUES (?,'ACTIVE',1,?,?,?)
            """, request.getId(), request.getStatus().name(), Timestamp.from(time), request.getProcedureTypeId());
        append(request, null, 1, time, RequestKafkaEventV1.Type.REQUEST_CREATED);
    }
    public void changed(RequestEntity request, RequestStatus previous) {
        var rows = jdbc.query("SELECT last_version,last_status,last_occurred_at,tracking_status FROM bd_requests_kafka.stream WHERE request_id=?",
                (rs,n) -> new Stream(rs.getLong(1), rs.getString(2), rs.getTimestamp(3).toInstant(), rs.getString(4)), request.getId());
        Instant now = clock.instant().truncatedTo(ChronoUnit.MICROS);
        if (rows.isEmpty()) {
            jdbc.update("""
                INSERT INTO bd_requests_kafka.stream(request_id,tracking_status,last_version,last_status,last_occurred_at,procedure_id,blocked_reason)
                VALUES (?,'BLOCKED_HISTORY',0,?,?,?,'MISSING_CREATION_HISTORY')
                """, request.getId(), previous.name(), Timestamp.from(now), request.getProcedureTypeId());
            hold(request, previous, now, "MISSING_CREATION_HISTORY");
            return;
        }
        Stream stream = rows.getFirst();
        Instant time = now.isBefore(stream.time()) ? stream.time() : now;
        if (!stream.tracking().equals("ACTIVE") || !stream.status().equals(previous.name())) {
            jdbc.update("UPDATE bd_requests_kafka.stream SET tracking_status='BLOCKED_HISTORY',blocked_reason='UNVERIFIED_HISTORY_GAP' WHERE request_id=?", request.getId());
            hold(request, previous, time, "UNVERIFIED_HISTORY_GAP");
            return;
        }
        long version = Math.addExact(stream.version(), 1);
        jdbc.update("UPDATE bd_requests_kafka.stream SET last_version=?,last_status=?,last_occurred_at=? WHERE request_id=?",
                version, request.getStatus().name(), Timestamp.from(time), request.getId());
        append(request, previous, version, time, RequestKafkaEventV1.Type.REQUEST_STATUS_CHANGED);
    }
    private void append(RequestEntity request, RequestStatus previous, long version, Instant time, RequestKafkaEventV1.Type type) {
        jdbc.update("""
            INSERT INTO bd_requests_kafka.outbox(event_id,request_id,request_version,event_type,previous_status,status,procedure_id,occurred_at,topic)
            VALUES (?,?,?,?,?,?,?,?,?)
            """, UUID.randomUUID(), request.getId(), version, type.name(), previous == null ? null : previous.name(),
                request.getStatus().name(), request.getProcedureTypeId(), Timestamp.from(time), properties.topic());
    }
    private void hold(RequestEntity request, RequestStatus previous, Instant time, String reason) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
            INSERT INTO bd_requests_kafka.history_hold(event_id,request_id,procedure_id,previous_status,status,occurred_at,reason)
            VALUES (?,?,?,?,?,?,?)
            """, id, request.getId(), request.getProcedureTypeId(), previous.name(), request.getStatus().name(), Timestamp.from(time), reason);
        LoggerFactory.getLogger(getClass()).warn("kafka_history_held eventId={} requestId={} reason={}", id, request.getId(), reason);
    }
    private record Stream(long version, String status, Instant time, String tracking) { }
}
