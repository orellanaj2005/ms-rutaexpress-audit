package cl.rutaexpress.audit.kafka;

import cl.rutaexpress.audit.dto.EventEnvelope;
import cl.rutaexpress.audit.service.AuditTimelineService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consume shipments.events y audit.timeline. Solo recibe, deja el contexto de log (MDC),
 * delega en {@link AuditTimelineService} y hace ack.
 *
 * Si el servicio lanza una excepción NO se hace ack: el error handler (KafkaConsumerConfig)
 * decide si reintenta (errores transitorios) o manda directo a audit.DLT (evento inválido).
 */
@Component
public class AuditEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuditEventConsumer.class);

    private final AuditTimelineService service;

    public AuditEventConsumer(AuditTimelineService service) {
        this.service = service;
    }

    @KafkaListener(
            topics = {"shipments.events", "audit.timeline"},
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onMessage(ConsumerRecord<String, EventEnvelope> record, Acknowledgment ack) {
        EventEnvelope envelope = record.value();
        putMdc(record, envelope);
        try {
            AuditTimelineService.Result result = service.record(envelope, record.topic());
            log.info("Evento {} tipo={} -> {}", envelope.eventId(), envelope.type(), result);
            ack.acknowledge();
        } catch (DataIntegrityViolationException dup) {
            // Carrera entre dos hilos/instancias con el mismo eventId (UQ_AUDIT_EVENT_ID): no es un error real.
            log.info("Evento {} ya existía (constraint única), se ignora", envelope.eventId());
            ack.acknowledge();
        } finally {
            MDC.clear();
        }
    }

    private static void putMdc(ConsumerRecord<String, EventEnvelope> record, EventEnvelope envelope) {
        MDC.put("topic", record.topic());
        MDC.put("partition", String.valueOf(record.partition()));
        MDC.put("offset", String.valueOf(record.offset()));
        if (envelope != null) {
            if (envelope.eventId() != null) {
                MDC.put("eventId", envelope.eventId());
            }
            if (envelope.traceId() != null) {
                MDC.put("traceId", envelope.traceId());
            }
        }
    }
}
