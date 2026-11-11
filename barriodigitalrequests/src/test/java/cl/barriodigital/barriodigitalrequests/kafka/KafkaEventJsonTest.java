package cl.barriodigital.barriodigitalrequests.kafka;

import cl.barriodigital.barriodigitalrequests.model.RequestStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Validation;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class KafkaEventJsonTest {
    final KafkaEventJson json = new KafkaEventJson(Validation.buildDefaultValidatorFactory().getValidator());
    @Test void emitsOnlyTheExactReportFieldsAndIsoUtc() throws Exception {
        var event = new RequestKafkaEventV1(UUID.randomUUID(), RequestKafkaEventV1.Type.REQUEST_CREATED, 1,
                1L, 1L, "12", "Permiso municipal", null, RequestStatus.INGRESADO, Instant.parse("2026-11-08T12:00:00Z"));
        var tree = new ObjectMapper().readTree(json.encode(event));
        List<String> fields = new ArrayList<>(); tree.fieldNames().forEachRemaining(fields::add);
        assertThat(fields).containsExactlyInAnyOrder("eventId","eventType","schemaVersion","requestId","requestVersion",
                "procedureId","procedureName","previousStatus","status","occurredAt");
        assertThat(tree.get("previousStatus").isNull()).isTrue();
        assertThat(tree.get("occurredAt").asText()).isEqualTo("2026-11-08T12:00:00Z");
    }
    @Test void incompleteOrOutOfContractEventCannotBeSent() {
        for (String name : new String[]{null,"","a".repeat(251)}) {
            var event = new RequestKafkaEventV1(UUID.randomUUID(), RequestKafkaEventV1.Type.REQUEST_CREATED, 1,
                    1L, 1L, "12", name, null, RequestStatus.INGRESADO, Instant.now());
            assertThatThrownBy(() -> json.encode(event)).isInstanceOf(IllegalArgumentException.class);
        }
        var wrong = new RequestKafkaEventV1(UUID.randomUUID(), RequestKafkaEventV1.Type.REQUEST_STATUS_CHANGED, 1,
                1L, 1L, "12", "Real name", RequestStatus.INGRESADO, RequestStatus.ADMITIDO, Instant.now());
        assertThatThrownBy(() -> json.encode(wrong)).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void backoffIsBounded() {
        var p = properties("http://localhost:8082", 100);
        assertThat(p.retryDelay(1)).isEqualTo(2000);
        assertThat(p.retryDelay(2)).isEqualTo(4000);
        assertThat(p.retryDelay(100)).isEqualTo(60000);
    }
    static KafkaPublisherProperties properties(String catalog, int timeout) {
        return new KafkaPublisherProperties(true,"bd.requests.events.v1",catalog,timeout,10,20000,2000,60000);
    }
}
