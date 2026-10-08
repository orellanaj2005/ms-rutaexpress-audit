package cl.rutaexpress.audit.messaging.deadletter;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Interpreta la cabecera x-death que agrega RabbitMQ cada vez que un mensaje muere.
 *
 * Un mensaje que pasó por los reintentos de notify trae varias entradas: una por cada
 * vez que expiró en la cola de reintento (reason=expired) y una por el rechazo final
 * en la cola principal (reason=rejected). La entrada que importa es la del rechazo;
 * por eso no sirven x-first-death-queue ni x-first-death-reason, que apuntan a la
 * primera expiración en la cola de reintento.
 */
@Component
public class XDeathParser {

    /** Cabecera con la que notify cuenta sus reintentos. */
    static final String RETRY_COUNT_HEADER = "x-retry-count";

    public DeadLetterInfo parse(MessageProperties properties) {
        List<Map<String, ?>> xDeath = properties.getXDeathHeader();
        Map<String, ?> entry = selectEntry(xDeath);

        String queue = entry != null ? asString(entry.get("queue")) : properties.getConsumerQueue();
        String exchange = entry != null ? asString(entry.get("exchange")) : null;
        String reason = entry != null ? asString(entry.get("reason")) : "desconocido";
        String routingKey = entry != null ? firstRoutingKey(entry.get("routing-keys")) : null;
        if (routingKey == null) {
            routingKey = properties.getReceivedRoutingKey();
        }
        Instant deadAt = entry != null && entry.get("time") instanceof Date date ? date.toInstant() : Instant.now();

        return new DeadLetterInfo(queue, exchange, routingKey, reason, attempts(properties, entry), deadAt);
    }

    private Map<String, ?> selectEntry(List<Map<String, ?>> xDeath) {
        if (xDeath == null || xDeath.isEmpty()) {
            return null;
        }
        // La lista viene ordenada de la muerte más reciente a la más antigua.
        return xDeath.stream()
                .filter(e -> "rejected".equals(asString(e.get("reason"))))
                .findFirst()
                .orElse(xDeath.get(0));
    }

    private int attempts(MessageProperties properties, Map<String, ?> entry) {
        // Intentos = el primero + los reintentos que hizo notify.
        Object retries = properties.getHeaders().get(RETRY_COUNT_HEADER);
        if (retries instanceof Number n) {
            return n.intValue() + 1;
        }
        if (entry != null && entry.get("count") instanceof Number n) {
            return n.intValue();
        }
        return 1;
    }

    private String firstRoutingKey(Object value) {
        if (value instanceof List<?> keys && !keys.isEmpty()) {
            return asString(keys.get(0));
        }
        return null;
    }

    private String asString(Object value) {
        return value != null ? value.toString() : null;
    }
}