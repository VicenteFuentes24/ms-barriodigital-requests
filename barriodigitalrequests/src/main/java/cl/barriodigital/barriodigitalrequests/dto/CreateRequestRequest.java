package cl.barriodigital.barriodigitalrequests.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CreateRequestRequest(
        @NotBlank(message = "procedureTypeId es obligatorio")
        @Size(max = 120, message = "procedureTypeId no puede superar 120 caracteres")
        String procedureTypeId,

        @NotBlank(message = "description es obligatoria")
        @Size(max = 2000, message = "description no puede superar 2000 caracteres")
        String description) {
}
