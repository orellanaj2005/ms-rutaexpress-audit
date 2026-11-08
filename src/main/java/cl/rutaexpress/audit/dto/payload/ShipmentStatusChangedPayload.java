package cl.rutaexpress.audit.dto.payload;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;

/** Payload de {@code ShipmentStatusChanged}. El nuevo estado viaja en {@code status}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ShipmentStatusChangedPayload(
        @NotBlank String shipmentId,
        @NotBlank String status,
        String previousStatus,
        String actor
) {
}


