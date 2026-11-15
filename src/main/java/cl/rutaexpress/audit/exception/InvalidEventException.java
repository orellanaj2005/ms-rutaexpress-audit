package cl.rutaexpress.audit.exception;

/**
 * Evento mal formado (campos obligatorios ausentes, payload que no calza con su tipo, etc.).
 * Reintentar no sirve: el mensaje seguirá igual de malo. Por eso está configurada como
 * "no reintentable" en KafkaConsumerConfig y el mensaje va directo a la DLT.
 */
public class InvalidEventException extends RuntimeException {

    public InvalidEventException(String message) {
        super(message);
    }

    public InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }
}

