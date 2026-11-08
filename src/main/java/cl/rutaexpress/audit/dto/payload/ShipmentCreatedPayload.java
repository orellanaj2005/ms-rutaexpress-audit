package cl.rutaexpress.audit.dto.payload;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;

/** Payload de {@code ShipmentCreated}. Se ignoran los campos que audit no usa. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ShipmentCreatedPayload(
        @NotBlank String shipmentId,
        String serviceType,
        String userId
) {
}


