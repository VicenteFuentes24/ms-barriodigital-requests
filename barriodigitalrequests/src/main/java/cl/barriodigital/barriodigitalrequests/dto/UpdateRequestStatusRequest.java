package cl.barriodigital.barriodigitalrequests.dto;

import cl.barriodigital.barriodigitalrequests.model.RequestStatus;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record UpdateRequestStatusRequest(
        @NotNull(message = "status es obligatorio")
        RequestStatus status,

        @Size(max = 1000, message = "comment no puede superar 1000 caracteres")
        String comment) {
}
