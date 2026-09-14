package cl.rutaexpress.audit.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

/**
 * Contrato común acordado con el equipo (punto de sincronización #3 del reparto
 * de carga de trabajo) para todos los eventos publicados en Kafka/RabbitMQ:
 * type, eventId, timestamp, traceId, correlationId + payload propio del dominio.
 *
 * Si el envelope real que arma Jassack en "shipments" difiere en algún nombre
 * de campo, ajusta este DTO — es el único lugar que hay que tocar.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EventEnvelope {

    private String type;
    private String eventId;
    private Instant timestamp;
    private String traceId;
    private String correlationId;
    private JsonNode payload;

    public EventEnvelope() {
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public JsonNode getPayload() {
        return payload;
    }

    public void setPayload(JsonNode payload) {
        this.payload = payload;
    }
}
