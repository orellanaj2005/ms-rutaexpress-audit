package cl.rutaexpress.audit.domain;

import jakarta.persistence.*;

import java.time.Instant;

/**
 * Registro de un mensaje de RabbitMQ que terminó en una DLQ.
 */
@Entity
@Table(name = "DEAD_LETTER_EVENT")
public class DeadLetterEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "dead_letter_event_seq")
    @SequenceGenerator(name = "dead_letter_event_seq", sequenceName = "DEAD_LETTER_EVENT_SEQ", allocationSize = 50)
    @Column(name = "ID")
    private Long id;

    @Column(name = "EVENT_ID", length = 100)
    private String eventId;

    @Column(name = "SOURCE_QUEUE", nullable = false, length = 200)
    private String sourceQueue;

    @Column(name = "SOURCE_EXCHANGE", length = 200)
    private String sourceExchange;

    @Column(name = "ROUTING_KEY", length = 200)
    private String routingKey;

    @Column(name = "REASON", nullable = false, length = 50)
    private String reason;

    @Column(name = "ATTEMPTS", nullable = false)
    private Integer attempts;

    @Column(name = "DEAD_AT", nullable = false)
    private Instant deadAt;

    @Column(name = "RECEIVED_AT", nullable = false)
    private Instant receivedAt;

    @Lob
    @Column(name = "RAW_PAYLOAD")
    private String rawPayload;

    protected DeadLetterEvent() {
    }

    public DeadLetterEvent(String eventId, String sourceQueue, String sourceExchange, String routingKey,
                           String reason, Integer attempts, Instant deadAt, String rawPayload) {
        this.eventId = eventId;
        this.sourceQueue = sourceQueue;
        this.sourceExchange = sourceExchange;
        this.routingKey = routingKey;
        this.reason = reason;
        this.attempts = attempts;
        this.deadAt = deadAt;
        this.receivedAt = Instant.now();
        this.rawPayload = rawPayload;
    }

    public Long getId() { return id; }
    public String getEventId() { return eventId; }
    public String getSourceQueue() { return sourceQueue; }
    public String getSourceExchange() { return sourceExchange; }
    public String getRoutingKey() { return routingKey; }
    public String getReason() { return reason; }
    public Integer getAttempts() { return attempts; }
    public Instant getDeadAt() { return deadAt; }
    public Instant getReceivedAt() { return receivedAt; }
    public String getRawPayload() { return rawPayload; }
}