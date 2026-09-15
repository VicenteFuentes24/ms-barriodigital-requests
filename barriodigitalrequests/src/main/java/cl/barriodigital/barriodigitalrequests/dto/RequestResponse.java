package cl.barriodigital.barriodigitalrequests.dto;

import java.time.Instant;

import cl.barriodigital.barriodigitalrequests.model.RequestStatus;

public record RequestResponse(
        Long id,
        String procedureTypeId,
        String description,
        RequestStatus status,
        String createdBy,
        Instant createdAt,
        Instant updatedAt) {
}
