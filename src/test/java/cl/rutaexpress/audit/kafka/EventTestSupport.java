package cl.rutaexpress.audit.kafka;

import cl.rutaexpress.audit.dto.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.validation.Validation;

import java.time.Instant;

/** Utilidades compartidas por las pruebas: validator real y constructor de envelopes. */
public final class EventTestSupport {

    public static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    public static final Instant TS = Instant.parse("2026-11-05T14:30:00Z");

    private EventTestSupport() {
    }

    public static EventValidator validator() {
        return new EventValidator(Validation.buildDefaultValidatorFactory().getValidator(), MAPPER);
    }

    public static EventEnvelope envelope(String eventId, String type, String payloadJson) {
        try {
            return new EventEnvelope(eventId, type, 1, "SHP-1", TS, "trace-1", "corr-1", "shipments",
                    MAPPER.readTree(payloadJson));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
