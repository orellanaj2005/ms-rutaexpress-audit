package cl.rutaexpress.audit.dto;

import cl.rutaexpress.audit.domain.AuditEvent;

import java.time.Instant;

public record AuditEventResponse(
        Long id,
        String eventId,
        String eventType,
        String shipmentId,
        String actor,
        Instant occurredAt,
        String traceId,
        String correlationId
) {
    public static AuditEventResponse from(AuditEvent e) {
        return new AuditEventResponse(
                e.getId(), e.getEventId(), e.getEventType(), e.getShipmentId(),
                e.getActor(), e.getOccurredAt(), e.getTraceId(), e.getCorrelationId()
        );
    }
}
