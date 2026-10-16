package cl.barriodigital.barriodigitalrequests.dto;

import java.time.Instant;

import cl.barriodigital.barriodigitalrequests.model.RequestStatus;

public record RequestEvent(
        Type type,
        String eventId,
        Instant timestamp,
        String traceId,
        String correlationId,
        Long requestId,
        String procedureTypeId,
        RequestStatus status,
        String createdBy) {

    public enum Type {
        REQUEST_CREATED,
        REQUEST_STATUS_CHANGED
    }
}
