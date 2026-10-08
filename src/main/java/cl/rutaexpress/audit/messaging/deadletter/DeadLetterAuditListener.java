package cl.rutaexpress.audit.messaging.deadletter;

import cl.rutaexpress.audit.domain.DeadLetterEvent;
import cl.rutaexpress.audit.repository.DeadLetterEventRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Consume q.audit.deadletter: cada mensaje que muere en una cola de comandos llega
 * aquí como copia. Se deja un log ERROR con la cola de origen, el motivo y los
 * intentos, y se guarda el registro en DEAD_LETTER_EVENT.
 *
 * ACK manual. Si la base de datos falla, el mensaje se devuelve a la cola una sola
 * vez; si vuelve a fallar se confirma igual, porque el original sigue en su DLQ y
 * reencolarlo sin límite solo llenaría el log mientras Oracle no responda.
 */
@Component
public class DeadLetterAuditListener {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterAuditListener.class);

    private final DeadLetterEventRepository repository;
    private final XDeathParser xDeathParser;
    private final ObjectMapper objectMapper;

    public DeadLetterAuditListener(DeadLetterEventRepository repository,
                                   XDeathParser xDeathParser,
                                   ObjectMapper objectMapper) {
        this.repository = repository;
        this.xDeathParser = xDeathParser;
        this.objectMapper = objectMapper;
    }

    @RabbitListener(queues = "${rutaexpress.rabbitmq.audit.dead-letter-queue}", ackMode = "MANUAL")
    public void onDeadLetter(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String body = new String(message.getBody(), StandardCharsets.UTF_8);
        DeadLetterInfo info = xDeathParser.parse(message.getMessageProperties());
        String eventId = extractEventId(body);

        log.error("Mensaje enviado a DLQ: eventId={} cola={} routingKey={} motivo={} intentos={}",
                eventId, info.queue(), info.routingKey(), info.reason(), info.attempts());

        try {
            repository.save(new DeadLetterEvent(
                    eventId, info.queue(), info.exchange(), info.routingKey(),
                    info.reason(), info.attempts(), info.deadAt(), body));
            channel.basicAck(deliveryTag, false);
        } catch (Exception ex) {
            if (!Boolean.TRUE.equals(message.getMessageProperties().isRedelivered())) {
                log.warn("No se pudo guardar el mensaje muerto eventId={}, se reintenta una vez: {}",
                        eventId, ex.getMessage());
                channel.basicNack(deliveryTag, false, true);
            } else {
                log.error("No se pudo guardar el mensaje muerto eventId={} tras reintentar; queda solo en su DLQ: {}",
                        eventId, ex.getMessage(), ex);
                channel.basicAck(deliveryTag, false);
            }
        }
    }

    /** El cuerpo puede no ser JSON válido (justamente por eso pudo morir). */
    private String extractEventId(String body) {
        try {
            JsonNode node = objectMapper.readTree(body);
            return node != null && node.hasNonNull("eventId") ? node.get("eventId").asText() : null;
        } catch (IOException e) {
            return null;
        }
    }
}