package cl.barriodigital.barriodigitalrequests.service;

import cl.barriodigital.barriodigitalrequests.dto.RequestResponse;
import cl.barriodigital.barriodigitalrequests.model.RequestEntity;
import org.springframework.stereotype.Component;

@Component
public class RequestMapper {

    public RequestResponse toResponse(RequestEntity entity) {
        return new RequestResponse(
                entity.getId(),
                entity.getProcedureTypeId(),
                entity.getDescription(),
                entity.getStatus(),
                entity.getCreatedBy(),
                entity.getCreatedAt(),
                entity.getUpdatedAt());
    }
}
