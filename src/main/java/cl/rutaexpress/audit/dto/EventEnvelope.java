package cl.rutaexpress.audit.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;

/**
 * Envelope común de todos los eventos Kafka (plan Kafka, sección 3):
 * eventId, type, version, aggregateId (también es la key del mensaje), timestamp ISO-8601,
 * traceId / correlationId, producer y payload propio del tipo de evento.
 *
 * El payload se deja como JsonNode y se convierte a su record tipado según {@code type}
 * (ver EventValidator#payload). Así un tipo desconocido o nuevo no rompe la deserialización
 * del envelope completo.
 *
 * Obligatorios: eventId, type, timestamp, payload. El resto es opcional para no mandar a la
 * DLT eventos válidos de un productor que todavía no los llena.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record EventEnvelope(
        @NotBlank String eventId,
        @NotBlank String type,
        Integer version,
        String aggregateId,
        @NotNull Instant timestamp,
        String traceId,
        String correlationId,
        String producer,
        @NotNull JsonNode payload
) {
}
