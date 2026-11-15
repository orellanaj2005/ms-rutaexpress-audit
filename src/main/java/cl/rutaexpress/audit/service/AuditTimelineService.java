package cl.rutaexpress.audit.service;

import cl.rutaexpress.audit.domain.AuditEvent;
import cl.rutaexpress.audit.dto.EventEnvelope;
import cl.rutaexpress.audit.dto.payload.AuditActionPayload;
import cl.rutaexpress.audit.dto.payload.ShipmentCreatedPayload;
import cl.rutaexpress.audit.dto.payload.ShipmentStatusChangedPayload;
import cl.rutaexpress.audit.kafka.EventTypes;
import cl.rutaexpress.audit.kafka.EventValidator;
import cl.rutaexpress.audit.repository.AuditEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Lógica de negocio del timeline de auditoría: valida el evento, lo deduplica por eventId
 * y lo guarda. El listener de Kafka solo recibe y hace ack; todo lo demás vive acá.
 */
@Service
public class AuditTimelineService {

    public enum Result { SAVED, DUPLICATE }

    private static final Logger log = LoggerFactory.getLogger(AuditTimelineService.class);

    private final AuditEventRepository repository;
    private final EventValidator validator;

    public AuditTimelineService(AuditEventRepository repository, EventValidator validator) {
        this.repository = repository;
        this.validator = validator;
    }

    /**
     * @throws cl.rutaexpress.audit.exception.InvalidEventException si el evento no cumple el contrato
     *         (no reintentable: va directo a la DLT)
     */
    @Transactional
    public Result record(EventEnvelope envelope, String sourceTopic) {
        validator.validateEnvelope(envelope);

        if (repository.existsByEventId(envelope.eventId())) {
            log.info("Evento {} ya procesado (dedup), se ignora", envelope.eventId());
            return Result.DUPLICATE;
        }

        String shipmentId = envelope.aggregateId();
        String actor = null;

        switch (envelope.type()) {
            case EventTypes.SHIPMENT_CREATED -> {
                ShipmentCreatedPayload p = validator.payload(envelope, ShipmentCreatedPayload.class);
                shipmentId = p.shipmentId();
                actor = p.userId();
            }
            case EventTypes.SHIPMENT_STATUS_CHANGED -> {
                ShipmentStatusChangedPayload p = validator.payload(envelope, ShipmentStatusChangedPayload.class);
                shipmentId = p.shipmentId();
                actor = p.actor();
            }
            case EventTypes.AUDIT_ACTION -> {
                AuditActionPayload p = validator.payload(envelope, AuditActionPayload.class);
                if (p.shipmentId() != null) {
                    shipmentId = p.shipmentId();
                }
                actor = p.actor();
            }
            // Un tipo nuevo igual se guarda: el timeline no debe perder eventos que todavía no conoce.
            default -> log.info("Tipo {} sin payload tipado, se guarda solo con los datos del envelope",
                    envelope.type());
        }

        repository.saveAndFlush(new AuditEvent(
                envelope.eventId(),
                envelope.type(),
                sourceTopic,
                shipmentId,
                actor,
                envelope.timestamp(),
                envelope.traceId(),
                envelope.correlationId(),
                envelope.payload().toString()));
        return Result.SAVED;
    }
}
