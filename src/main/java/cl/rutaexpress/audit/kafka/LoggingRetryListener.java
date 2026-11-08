package cl.rutaexpress.audit.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.RetryListener;

/** Deja en el log cada reintento y el momento en que el mensaje se manda a la DLT. */
public class LoggingRetryListener implements RetryListener {

    private static final Logger log = LoggerFactory.getLogger(LoggingRetryListener.class);

    @Override
    public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
        log.warn("Fallo procesando {}-{}@{} (intento {}): {}",
                record.topic(), record.partition(), record.offset(), deliveryAttempt, rootMessage(ex));
    }

    @Override
    public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
        log.error("Reintentos agotados o error no reintentable: {}-{}@{} se envía a la DLT. Causa: {}",
                record.topic(), record.partition(), record.offset(), rootMessage(ex));
    }

    @Override
    public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
        log.error("NO se pudo enviar {}-{}@{} a la DLT: {}",
                record.topic(), record.partition(), record.offset(), failure.getMessage(), failure);
    }

    private static String rootMessage(Throwable ex) {
        Throwable root = ex;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }
}
