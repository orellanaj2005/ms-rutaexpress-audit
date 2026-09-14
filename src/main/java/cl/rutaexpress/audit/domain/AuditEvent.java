package cl.rutaexpress.audit.domain;

import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "AUDIT_EVENT", uniqueConstraints = {
        @UniqueConstraint(name = "UQ_AUDIT_EVENT_ID", columnNames = "EVENT_ID")
})
public class AuditEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "audit_event_seq")
    @SequenceGenerator(name = "audit_event_seq", sequenceName = "AUDIT_EVENT_SEQ", allocationSize = 50)
    @Column(name = "ID")
    private Long id;

    // Idempotencia: dedup por eventId (igual criterio que usó Jassack en notify)
    @Column(name = "EVENT_ID", nullable = false, length = 100)
    private String eventId;

    @Column(name = "EVENT_TYPE", nullable = false, length = 100)
    private String eventType;

    @Column(name = "SOURCE_TOPIC", nullable = false, length = 100)
    private String sourceTopic;

    @Column(name = "SHIPMENT_ID", length = 100)
    private String shipmentId;

    @Column(name = "ACTOR", length = 150)
    private String actor;

    @Column(name = "OCCURRED_AT", nullable = false)
    private Instant occurredAt;

    @Column(name = "TRACE_ID", length = 100)
    private String traceId;

    @Column(name = "CORRELATION_ID", length = 100)
    private String correlationId;

    @Lob
    @Column(name = "RAW_PAYLOAD")
    private String rawPayload;

    protected AuditEvent() {
    }

    public AuditEvent(String eventId, String eventType, String sourceTopic, String shipmentId,
                       String actor, Instant occurredAt, String traceId, String correlationId,
                       String rawPayload) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.sourceTopic = sourceTopic;
        this.shipmentId = shipmentId;
        this.actor = actor;
        this.occurredAt = occurredAt;
        this.traceId = traceId;
        this.correlationId = correlationId;
        this.rawPayload = rawPayload;
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public String getShipmentId() {
        return shipmentId;
    }

    public String getActor() {
        return actor;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public String getTraceId() {
        return traceId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getRawPayload() {
        return rawPayload;
    }
}
