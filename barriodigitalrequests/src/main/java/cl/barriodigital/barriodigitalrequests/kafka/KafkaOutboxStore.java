package cl.barriodigital.barriodigitalrequests.kafka;

import cl.barriodigital.barriodigitalrequests.model.RequestStatus;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@ConditionalOnProperty(name = "barriodigital.kafka.enabled", havingValue = "true")
public class KafkaOutboxStore {
    private final JdbcTemplate jdbc;
    public KafkaOutboxStore(JdbcTemplate jdbc) { this.jdbc = jdbc; }
    public Optional<Row> next(boolean prepared, Instant now) {
        // Lock only the queue row. Never hold the Request row while contacting Catalog/Kafka.
        String sql = """
            SELECT o.*,s.procedure_name FROM bd_requests_kafka.outbox o
            JOIN bd_requests_kafka.stream s ON s.request_id=o.request_id
            WHERE o.published_at IS NULL AND s.tracking_status='ACTIVE' AND o.available_at<=?
              AND NOT EXISTS (SELECT 1 FROM bd_requests_kafka.outbox older
                WHERE older.request_id=o.request_id AND older.request_version<o.request_version AND older.published_at IS NULL)
            """ + (prepared ? " AND o.payload IS NOT NULL " : " AND o.payload IS NULL ") +
                " ORDER BY o.occurred_at,o.request_id,o.request_version LIMIT 1 FOR UPDATE OF o SKIP LOCKED";
        return jdbc.query(sql, (rs,n) -> new Row(rs.getObject("event_id", UUID.class), rs.getLong("request_id"),
                rs.getLong("request_version"), RequestKafkaEventV1.Type.valueOf(rs.getString("event_type")),
                rs.getString("procedure_id"), rs.getString("procedure_name"),
                rs.getString("previous_status") == null ? null : RequestStatus.valueOf(rs.getString("previous_status")),
                RequestStatus.valueOf(rs.getString("status")), rs.getTimestamp("occurred_at").toInstant(),
                rs.getString("topic"), rs.getString("payload"), rs.getInt("attempts")), Timestamp.from(now)).stream().findFirst();
    }
    public void prepared(Row row, String name, String payload) {
        jdbc.update("UPDATE bd_requests_kafka.stream SET procedure_name=COALESCE(procedure_name,?) WHERE request_id=?", name, row.requestId());
        jdbc.update("UPDATE bd_requests_kafka.outbox SET payload=?,last_error=NULL WHERE event_id=? AND payload IS NULL", payload, row.eventId());
    }
    public void published(Row row, Instant now) {
        jdbc.update("UPDATE bd_requests_kafka.outbox SET published_at=?,last_error=NULL WHERE event_id=?", Timestamp.from(now), row.eventId());
    }
    public void retry(Row row, Instant available, String reason) {
        jdbc.update("UPDATE bd_requests_kafka.outbox SET attempts=attempts+1,available_at=?,last_error=? WHERE event_id=?",
                Timestamp.from(available), reason, row.eventId());
    }
    public record Row(UUID eventId, long requestId, long version, RequestKafkaEventV1.Type type,
                      String procedureId, String procedureName, RequestStatus previousStatus, RequestStatus status,
                      Instant occurredAt, String topic, String payload, int attempts) {
        public RequestKafkaEventV1 event(String name) {
            return new RequestKafkaEventV1(eventId, type, 1, requestId, version, procedureId, name, previousStatus, status, occurredAt);
        }
    }
}
