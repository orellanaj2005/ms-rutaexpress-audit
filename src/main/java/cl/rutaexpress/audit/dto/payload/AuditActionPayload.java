package cl.rutaexpress.audit.dto.payload;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;

/** Payload de {@code AuditAction} (acciones administrativas publicadas en audit.timeline). */
@JsonIgnoreProperties(ignoreUnknown = true)
public record AuditActionPayload(
        @NotBlank String action,
        @NotBlank String actor,
        String shipmentId
) {
}

