package cl.rutaexpress.audit.kafka;

import cl.rutaexpress.audit.dto.EventEnvelope;
import cl.rutaexpress.audit.exception.InvalidEventException;
import org.junit.jupiter.api.Test;

import static cl.rutaexpress.audit.kafka.EventTestSupport.MAPPER;
import static cl.rutaexpress.audit.kafka.EventTestSupport.TS;
import static cl.rutaexpress.audit.kafka.EventTestSupport.envelope;
import static cl.rutaexpress.audit.kafka.EventTestSupport.validator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventValidatorTest {

    private final EventValidator validator = validator();

    @Test
    void envelopeValidoPasa() {
        validator.validateEnvelope(envelope("evt-1", "ShipmentCreated", "{\"shipmentId\":\"SHP-1\"}"));
    }

    @Test
    void envelopeNuloSeRechaza() {
        assertThatThrownBy(() -> validator.validateEnvelope(null))
                .isInstanceOf(InvalidEventException.class);
    }

    @Test
    void envelopeSinEventIdSeRechaza() {
        assertThatThrownBy(() -> validator.validateEnvelope(
                envelope(null, "ShipmentCreated", "{\"shipmentId\":\"SHP-1\"}")))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining("eventId");
    }

    @Test
    void envelopeSinPayloadSeRechaza() {
        EventEnvelope sinPayload = new EventEnvelope("evt-1", "ShipmentCreated", 1, "SHP-1", TS,
                null, null, null, null);
        assertThatThrownBy(() -> validator.validateEnvelope(sinPayload))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining("payload");
    }

    @Test
    void payloadQueNoCalzaConElTipoSeRechaza() throws Exception {
        EventEnvelope env = new EventEnvelope("evt-1", "ShipmentCreated", 1, "SHP-1", TS,
                null, null, null, MAPPER.readTree("\"esto no es un objeto\""));
        assertThatThrownBy(() -> validator.payload(env, TestPayload.class))
                .isInstanceOf(InvalidEventException.class);
    }

    @Test
    void payloadConCampoObligatorioVacioSeRechaza() {
        EventEnvelope env = envelope("evt-1", "ShipmentCreated", "{\"name\":\"  \"}");
        assertThatThrownBy(() -> validator.payload(env, TestPayload.class))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining("name");
    }

    @Test
    void payloadValidoSeConvierteIgnorandoCamposDesconocidos() {
        EventEnvelope env = envelope("evt-1", "ShipmentCreated", "{\"name\":\"ok\",\"extra\":123}");
        assertThat(validator.payload(env, TestPayload.class).name()).isEqualTo("ok");
    }

    @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
    record TestPayload(@jakarta.validation.constraints.NotBlank String name) {
    }
}
