package cl.rutaexpress.audit.messaging.deadletter;

import java.time.Instant;

/**
 * Datos extraídos de la cabecera x-death de un mensaje muerto.
 */
public record DeadLetterInfo(String queue, String exchange, String routingKey,
                             String reason, int attempts, Instant deadAt) {
}