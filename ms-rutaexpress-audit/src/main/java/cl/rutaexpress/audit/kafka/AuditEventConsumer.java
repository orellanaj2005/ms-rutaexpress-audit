package cl.rutaexpress.audit.kafka;

import cl.rutaexpress.audit.domain.AuditEvent;
import cl.rutaexpress.audit.dto.EventEnvelope;
import cl.rutaexpress.audit.repository.AuditEventRepository;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Consume shipments.events y audit.timeline (ver Caso 3, sección 9, y el
 * reparto de carga de trabajo: "Consume audit.timeline y probablemente
 * también shipments.events para nutrir el timeline").
 *
 * ACK manual + idempotencia por eventId, mismo criterio que notify aplicó
 * para RabbitMQ, para no duplicar filas si Kafka reentrega un mensaje.
 */
@Component
public class AuditEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuditEventConsumer.class);

    private final AuditEventRepository repository;

    public AuditEventConsumer(AuditEventRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(
            topics = {"shipments.events", "audit.timeline"},
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void onMessage(EventEnvelope envelope, Acknowledgment ack,
                           org.apache.kafka.clients.consumer.ConsumerRecord<String, EventEnvelope> record) {
        try {
            if (envelope.getEventId() == null) {
                log.warn("Evento sin eventId en tópico {}, se descarta: {}", record.topic(), envelope.getType());
                ack.acknowledge();
                return;
            }

            if (repository.existsByEventId(envelope.getEventId())) {
                log.info("Evento {} ya procesado (dedup), se ignora", envelope.getEventId());
                ack.acknowledge();
                return;
            }

            JsonNode payload = envelope.getPayload();
            String shipmentId = extractText(payload, "shipmentId");
            String actor = extractText(payload, "actor");
            if (actor == null) {
                actor = extractText(payload, "userId");
            }

            AuditEvent entity = new AuditEvent(
                    envelope.getEventId(),
                    envelope.getType(),
                    record.topic(),
                    shipmentId,
                    actor,
                    envelope.getTimestamp() != null ? envelope.getTimestamp() : java.time.Instant.now(),
                    envelope.getTraceId(),
                    envelope.getCorrelationId(),
                    payload != null ? payload.toString() : null
            );

            repository.save(entity);
            ack.acknowledge();
        } catch (DataIntegrityViolationException dup) {
            // Carrera entre dos particiones/instancias con el mismo eventId: no es un error real.
            log.info("Evento {} ya existía (constraint UQ_AUDIT_EVENT_ID), se ignora", envelope.getEventId());
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Error procesando evento {} del tópico {}: {}", envelope.getEventId(), record.topic(), ex.getMessage(), ex);
            // No se hace ack(): el listener container reintenta según su back-off configurado
            // y, tras agotar intentos, el error handler lo enviará al *.DLT correspondiente.
            throw ex;
        }
    }

    private String extractText(JsonNode payload, String field) {
        if (payload == null || !payload.hasNonNull(field)) {
            return null;
        }
        return payload.get(field).asText();
    }
}
