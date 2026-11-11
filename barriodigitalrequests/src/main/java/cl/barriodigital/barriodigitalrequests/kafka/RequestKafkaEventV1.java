package cl.barriodigital.barriodigitalrequests.kafka;

import cl.barriodigital.barriodigitalrequests.model.RequestStatus;
import java.time.Instant;
import java.util.UUID;
import jakarta.validation.constraints.*;
import com.fasterxml.jackson.annotation.JsonIgnore;

/** Kafka-only wire contract. The RabbitMQ RequestEvent is unchanged. */
public record RequestKafkaEventV1(
        @NotNull UUID eventId, @NotNull Type eventType,
        @NotNull @Min(1) @Max(1) Integer schemaVersion,
        @NotNull @Positive Long requestId, @NotNull @Positive Long requestVersion,
        @NotBlank @Size(max = 100) String procedureId,
        @NotBlank @Size(max = 250) String procedureName,
        RequestStatus previousStatus, @NotNull RequestStatus status, @NotNull Instant occurredAt) {
    public enum Type { REQUEST_CREATED, REQUEST_STATUS_CHANGED }
    @JsonIgnore @AssertTrue
    public boolean isConsistent() {
        if (eventType == null || requestVersion == null || status == null) return true;
        return eventType == Type.REQUEST_CREATED
                ? requestVersion == 1 && previousStatus == null && status == RequestStatus.INGRESADO
                : requestVersion > 1 && previousStatus != null && previousStatus != status;
    }
}
